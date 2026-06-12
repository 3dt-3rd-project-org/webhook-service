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
        nextStatus = 'ANALYZING_FINISHED';
      } else if (type === 'error') {
        nextStatus = 'ANALYZING_ERROR';
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
            eventType = 'ANALYZING_FINISHED';
            eventPayload.book = updatedBook;
          } else if (type === 'error') {
            eventType = 'ANALYZING_ERROR';
            eventPayload.error = error || null;
          } else {
            // 진행률(progress) 이벤트 처리
            eventType = 'ANALYZING';
            eventPayload.progress = progress !== undefined ? parseInt(progress, 10) : 0;
          }

          eventPayload.status = eventType;

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

app.http('handleMetadataComplete', {
  methods: ['POST'],
  authLevel: 'anonymous',
  route: 'webhook/metadata',
  handler: async (request, context) => {
    logger.info('[Webhook Metadata Handler] Functions 메타데이터 완료 웹훅 요청 접수');
    try {
      const reqBody = await request.json();
      const bookIdRaw = reqBody.books_id !== undefined ? reqBody.books_id : reqBody.book_id;
      const eventType = reqBody.event || 'metadata_done';

      if (bookIdRaw === undefined || bookIdRaw === null) {
        if (eventType === 'error') {
          logger.error(`[Webhook Metadata Error] 파이프라인에서 메타데이터 파싱 에러 수신: ${reqBody.error || '알 수 없는 오류'}`);
          return {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ message: '에러 웹훅 수신 및 로깅 완료' })
          };
        }

        return {
          status: 400,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ error: 'Bad Request', message: 'books_id 또는 book_id 필드는 필수입니다.' })
        };
      }

      const bookId = parseInt(bookIdRaw, 10);
      logger.info(`[Webhook] Azure Functions 메타데이터 작업 결과 수신 - Book ID: ${bookId}, Event: ${eventType}`);

      const result = await dbPool.query(
        `SELECT * FROM books WHERE books_id = $1`,
        [bookId]
      );

      if (result.rows.length === 0) {
        return {
          status: 404,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ error: 'Not Found', message: '해당 도서를 찾을 수 없습니다.' })
        };
      }

      const updatedBook = result.rows[0];
      logger.info(`[Webhook] Azure Functions가 DB에 직접 갱신한 최신 도서 정보 로드 완료 (제목: ${updatedBook.title})`);

      if (wpsClient) {
        try {
          await wpsClient.group(`admin_${updatedBook.admin_id}`).sendToAll({
            status: eventType === 'error' ? 'METADATA_ERROR' : 'METADATA_COMPLETE',
            book: updatedBook,
            error: reqBody.error || null
          });
          logger.info(`[Web PubSub Publish] 관리자 ${updatedBook.admin_id} 채널에 Book ${bookId} 메타데이터 결과 이벤트 전송 완료.`);
        } catch (wpsErr) {
          logger.error(`[Web PubSub Publish Warning] 실시간 브로드캐스트 실패: ${wpsErr.message}`);
        }
      }

      return handleSuccess({
        message: '도서 메타데이터 웹훅 처리를 성공적으로 완료했습니다.',
        book: updatedBook
      });
    } catch (err) {
      return handleError(err, logger, 'Webhook Metadata Handler');
    }
  }
});

app.http('handleSummaryWebhook', {
  methods: ['POST'],
  authLevel: 'anonymous',
  route: 'webhook/summary',
  handler: async (request, context) => {
    logger.info('[Webhook Summary Handler] ADF 요약 생성 파이프라인 웹훅 요청 접수');
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
      logger.info(`[Webhook Summary] 요약 파이프라인 신호 수신 - Book ID: ${bookId}, Type: ${type}, Step: ${step}`);

      // 1. 성공/실패/진행률에 따른 DB 상태 결정
      let nextStatus;
      if (type === 'success' && step === 'summary_complete') {
        nextStatus = 'SUMMARIZING_COMPLETE';
      } else if (type === 'error') {
        nextStatus = 'SUMMARY_ERROR';
      } else {
        nextStatus = 'SUMMARIZING';
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

          if (type === 'success' && step === 'summary_complete') {
            eventType = 'SUMMARIZING_COMPLETE';
            eventPayload.book = updatedBook;
          } else if (type === 'error') {
            eventType = 'SUMMARY_ERROR';
            eventPayload.error = error || null;
          } else {
            // 진행률(progress) 이벤트 처리
            eventType = 'SUMMARIZING';
            eventPayload.progress = progress !== undefined ? parseInt(progress, 10) : 0;
          }

          eventPayload.status = eventType;

          await wpsClient.group(`admin_${adminId}`).sendToAll(eventPayload);
          logger.info(`[Web PubSub Publish] 관리자 ${adminId} 채널에 요약 이벤트 ${eventType} 전송 완료.`);
        } catch (wpsErr) {
          logger.error(`[Web PubSub Publish Warning] 실시간 요약 브로드캐스트 실패: ${wpsErr.message}`);
        }
      }

      return handleSuccess({
        message: '요약 생성 파이프라인 결과 웹훅 처리가 성공적으로 완료되었습니다.',
        book_id: bookId,
        status: nextStatus
      });
    } catch (err) {
      return handleError(err, logger, 'Webhook Summary Handler');
    }
  }
});

