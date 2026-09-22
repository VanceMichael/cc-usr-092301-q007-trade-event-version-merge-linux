const express = require("express");
const app = express();
app.use(express.json());
app.get("/health", (_req, res) => res.json({ status: "ok" }));

if (require.main === module) {
  const port = Number(process.env.PORT || 3000);
  const host = process.env.HOST || "0.0.0.0";
  app.listen(port, host);
}

module.exports = app;
