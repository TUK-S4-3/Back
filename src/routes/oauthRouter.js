import express from "express";
import passport from "passport";

const router = express.Router();

router.get("/login/google", passport.authenticate("google"));

router.get(
  "/callback/google",
  (req, res, next) => {
    passport.authenticate("google", (err, user, info) => {
      if (err) {
        return res.status(500).json({
          ok: false,
          message: "oauth_google_callback_error",
          detail: err.message,
        });
      }

      if (!user) {
        return res.status(401).json({
          ok: false,
          message: "oauth_google_login_failed",
          detail: info?.message ?? null,
        });
      }

      return req.logIn(user, (loginErr) => {
        if (loginErr) {
          return res.status(500).json({
            ok: false,
            message: "oauth_session_save_failed",
            detail: loginErr.message,
          });
        }

        return res.redirect("/");
      });
    })(req, res, next);
  }
);



export default router;
