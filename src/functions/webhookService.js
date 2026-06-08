import { app } from '@azure/functions';
import { WebPubSubServiceClient } from '@azure/web-pubsub';
import { dbPool } from '../config/db.js';
import { logger } from '../utils/logger.js';
import { handleSuccess, handleError } from '../shared/responseHelper.js';

const wpsConnectionString = process.env.AZURE_WEB_PUBSUB_CONNECTION_STRING;
const wpsHub = process.env.AZURE_WEB_PUBSUB_HUB;

let wpsClient;
if (wpsConnectionString && wpsHub) {
  try {
    wpsClient = new WebPubSubServiceClient(wpsConnectionString, wpsHub);
  } catch (err) {
    logger.error(`[Web PubSub Client Init Failed]: ${err.message}`);
  }
}

app.http('handlePipelineWebhook', {
  methods: ['POST'],
  authLevel: 'anonymous',
  route: 'webhook/pipeline',
  handler: async (request, context) => {
    logger.info('[Webhook Pipeline Handler] ADF 통합 파이프라인 웹훅 요청 접수');
    try {
      const reqBody = await request.json();
      const { type, step, message, books_id, pipeline_run_id, progress, error } = reqBody;

      if (!books_id) {
        return {
          status: 400,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ error: 'Bad Request', message: 'books_id 필드는 필수입니다.' })
        };
      }

      const bookId = parseInt(books_id, 10);
      logger.info(`[Webhook] 파이프라인 신호 수신 - Book ID: ${bookId}, Type: ${type}, Step: ${step}`);

      // 1. 성공/실패/진행률에 따른 DB 상태 결정
      let nextStatus;
      if (type === 'success' && step === 'dag1_complete') {
        nextStatus = 'COMPLETE';
      } else if (type === 'error') {
        nextStatus = 'ERROR';
      } else {
        nextStatus = 'ANALYZING';
      }

      // PostgreSQL DB 상태 업데이트
      const dbRes = await dbPool.query(
        "UPDATE books SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE books_id = $2 RETURNING *",
        [nextStatus, bookId]
      );

      if (dbRes.rows.length === 0) {
        return {
          status: 404,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ error: 'Not Found', message: '해당 도서 정보를 찾을 수 없습니다.' })
        };
      }

      const updatedBook = dbRes.rows[0];
      const adminId = updatedBook.admin_id;

      // 2. Web PubSub 브로드캐스트 이벤트 매핑 및 전송
      if (wpsClient) {
        try {
          let eventPayload = {
            book_id: bookId,
            step: step || null,
            message: message || null
          };

          let eventType;

          if (type === 'success' && step === 'dag1_complete') {
            eventType = 'METADATA_COMPLETE';
            eventPayload.book = updatedBook;
          } else if (type === 'error') {
            eventType = 'METADATA_ERROR';
            eventPayload.error = error || null;
          } else {
            // 진행률(progress) 이벤트 처리
            eventType = 'PROGRESS';
            eventPayload.progress = progress !== undefined ? parseInt(progress, 10) : 0;
          }

          eventPayload.event = eventType;

          await wpsClient.group(`admin_${adminId}`).sendToAll(eventPayload);
          logger.info(`[Web PubSub Publish] 관리자 ${adminId} 채널에 이벤트 ${eventType} 전송 완료.`);
        } catch (wpsErr) {
          logger.error(`[Web PubSub Publish Warning] 실시간 브로드캐스트 실패: ${wpsErr.message}`);
        }
      }

      return handleSuccess({
        message: '통합 파이프라인 결과 웹훅 처리가 성공적으로 완료되었습니다.',
        book_id: bookId,
        status: nextStatus
      });
    } catch (err) {
      return handleError(err, logger, 'Webhook Pipeline Handler');
    }
  }
});
