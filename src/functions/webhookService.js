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

app.http('handleAdfProgress', {
  methods: ['POST'],
  authLevel: 'anonymous',
  route: 'webhook/adf-progress',
  handler: async (request, context) => {
    logger.info('[Webhook Progress Handler] ADF 분석 진행률 웹훅 요청 접수');
    try {
      const reqBody = await request.json();
      const { book_id, progress } = reqBody;

      if (book_id === undefined || progress === undefined) {
        return {
          status: 400,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ error: 'Bad Request', message: 'book_id와 progress 필드는 필수입니다.' })
        };
      }

      const bookId = parseInt(book_id, 10);
      const progNum = parseInt(progress, 10);

      logger.info(`[Webhook] 진행률 수신 - Book ID: ${bookId}, Progress: ${progNum}%`);

      if (progNum === 100) {
        logger.info(`[Webhook] Book ${bookId} 분석 100% 완료. PostgreSQL 상태 업데이트 수행.`);
        await dbPool.query(
          "UPDATE books SET status = 'COMPLETE', updated_at = CURRENT_TIMESTAMP WHERE books_id = $1",
          [bookId]
        );
      }

      // DB에서 admin_id 조회하여 관리자 고유 채널로 이벤트 브로드캐스트
      const bookRes = await dbPool.query('SELECT admin_id FROM books WHERE books_id = $1', [bookId]);
      if (bookRes.rows.length > 0) {
        const adminId = bookRes.rows[0].admin_id;

        if (wpsClient) {
          try {
            await wpsClient.group(`admin_${adminId}`).sendToAll({
              event: 'PROGRESS',
              book_id: bookId,
              progress: progNum
            });
            logger.info(`[Web PubSub Publish] 관리자 ${adminId} 채널에 Book ${bookId} 진행률 배달 성공: ${progNum}%`);
          } catch (wpsErr) {
            logger.error(`[Web PubSub Publish Warning] 실시간 브로드캐스트 실패: ${wpsErr.message}`);
          }
        }
      } else {
        logger.warn(`[Webhook Warning] Book ID ${bookId}에 해당하는 도서 정보를 조회할 수 없어 실시간 알림을 생략합니다.`);
      }

      return handleSuccess({
        message: '진행 상태 웹훅 처리가 성공적으로 수행되었습니다.',
        book_id: bookId,
        progress: progNum
      });
    } catch (err) {
      return handleError(err, logger, 'Webhook Progress Handler');
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
            event: eventType === 'error' ? 'METADATA_ERROR' : 'METADATA_COMPLETE',
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
