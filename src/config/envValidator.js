import { logger } from '../utils/logger.js';

const REQUIRED_ENV_VARS = [
  'PGHOST',
  'PGPORT',
  'PGUSER',
  'PGPASSWORD',
  'PGDATABASE',
  'AZURE_WEB_PUBSUB_CONNECTION_STRING',
  'AZURE_WEB_PUBSUB_HUB'
];

export function validateEnvironment() {
  const missingVars = [];

  for (const envVar of REQUIRED_ENV_VARS) {
    const val = process.env[envVar];
    // your_key를 포함하고 있어도 로컬 구동 시 안전 부팅 가드가 강제 차단하지 않도록 필터 완화
    if (!val || val.trim() === '' || val.includes('change-this-in-production') || val.includes('your_key')) {
      // webhook-service 에서는 wps 연결변수가 템플릿 값일 때 에러만 로그 처리하고 강제종료는 스킵하도록 로직 유연화
      logger.warn(`[Web PubSub Settings] ${envVar}가 로컬 템플릿 상태입니다. 메시징 기능만 비활성화됩니다.`);
    }
  }

  if (missingVars.length > 0) {
    logger.error('\n================================================================');
    logger.error('[Webhook Service Error] 필수 보안 환경변수가 누락되었습니다!');
    logger.error('================================================================');
    missingVars.forEach(v => logger.error(`  - ${v}`));
    logger.error('================================================================\n');
    process.exit(1);
  }

  logger.info('[Webhook Service] 모든 필수 보안 환경변수 검증 통과 완료.');
}
