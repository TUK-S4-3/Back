import express from "express";
import passport from "passport";

const router = express.Router();
const oauthSuccessRedirectUrl =
  process.env.OAUTH_SUCCESS_REDIRECT_URL ??
  process.env.FRONTEND_LOGIN_SUCCESS_URL ??
  "http://localhost:5173/auth/success";
const oauthFailureRedirectUrl =
  process.env.OAUTH_FAILURE_REDIRECT_URL ??
  process.env.FRONTEND_LOGIN_FAILURE_URL ??
  "http://localhost:5173/auth/failure";

function withReason(baseUrl, reason) {
  try {
    const url = new URL(baseUrl);
    if (reason) {
      url.searchParams.set("reason", reason);
    }
    return url.toString();
  } catch {
    return baseUrl;
  }
}

router.get("/login/google", passport.authenticate("google"));

router.get(
  "/callback/google",
  (req, res, next) => {
    passport.authenticate("google", (err, user, info) => {
      if (err) {
        return res.redirect(
          withReason(oauthFailureRedirectUrl, "oauth_google_callback_error")
        );
      }

      if (!user) {
        const reason = info?.message ? `oauth_google_login_failed:${info.message}` : "oauth_google_login_failed";
        return res.redirect(withReason(oauthFailureRedirectUrl, reason));
      }

      return req.logIn(user, (loginErr) => {
        if (loginErr) {
          return res.redirect(
            withReason(oauthFailureRedirectUrl, "oauth_session_save_failed")
          );
        }

        return res.redirect(oauthSuccessRedirectUrl);
      });
    })(req, res, next);
  }
);



export default router;
