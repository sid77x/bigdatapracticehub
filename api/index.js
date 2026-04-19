let appPromise;

module.exports = async (req, res) => {
  if (!appPromise) {
    appPromise = import("../backend/src/app.js").then((mod) => mod.getApp());
  }

  const app = await appPromise;
  return app(req, res);
};
