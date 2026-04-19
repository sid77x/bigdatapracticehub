import pino from "pino";
import { config } from "./config.js";
import { getApp } from "./app.js";

const logger = pino({ level: "info" });
const app = await getApp();

app.listen(config.port, () => {
  logger.info(`Backend running on http://localhost:${config.port}`);
});
