import { loadConfig } from './config.js';
import { runProjector } from './projector.js';

runProjector(loadConfig()).catch((error: unknown) => {
  console.error(`projector failed: ${String(error instanceof Error ? error.message : error)}`);
  process.exitCode = 1;
});
