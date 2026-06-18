/* ============================================================================
 * auth.js — shared Cognito authentication for Atomic Computing Enterprise Hub
 *
 * Uses amazon-cognito-identity-js (loaded from CDN in each page).
 * Provides: login, logout, token retrieval, tenant/role extraction,
 * and a guard that redirects to login.html when no valid session exists.
 *
 * After deploying the CDK stack, fill in the three values below from the
 * CloudFormation stack outputs (UserPoolId, UserPoolClientId, CognitoRegion).
 * ========================================================================== */

const COGNITO_CONFIG = {
  UserPoolId:  "REPLACE_WITH_UserPoolId",       // e.g. eu-central-1_AbC123xyz
  ClientId:    "REPLACE_WITH_UserPoolClientId", // e.g. 1a2b3c4d5e6f7g8h9i0j
  Region:      "eu-central-1",
};

const AtomicAuth = (function () {
  let _pool = null;

  function pool() {
    if (!_pool) {
      _pool = new AmazonCognitoIdentity.CognitoUserPool({
        UserPoolId: COGNITO_CONFIG.UserPoolId,
        ClientId:   COGNITO_CONFIG.ClientId,
      });
    }
    return _pool;
  }

  /** Sign in with email + password. Resolves with the ID token JWT. */
  function login(email, password) {
    return new Promise((resolve, reject) => {
      const user = new AmazonCognitoIdentity.CognitoUser({
        Username: email, Pool: pool(),
      });
      const details = new AmazonCognitoIdentity.AuthenticationDetails({
        Username: email, Password: password,
      });
      user.authenticateUser(details, {
        onSuccess: (session) => resolve(session.getIdToken().getJwtToken()),
        onFailure: (err) => reject(err),
        newPasswordRequired: (attrs) => {
          // First-login: Cognito requires a permanent password.
          reject({ code: "NewPasswordRequired", user, attrs });
        },
      });
    });
  }

  /** Complete the forced new-password challenge on first login. */
  function completeNewPassword(user, newPassword) {
    return new Promise((resolve, reject) => {
      user.completeNewPasswordChallenge(newPassword, {}, {
        onSuccess: (session) => resolve(session.getIdToken().getJwtToken()),
        onFailure: (err) => reject(err),
      });
    });
  }

  /** Returns the current valid ID token, refreshing if needed, or null. */
  function getToken() {
    return new Promise((resolve) => {
      const user = pool().getCurrentUser();
      if (!user) return resolve(null);
      user.getSession((err, session) => {
        if (err || !session || !session.isValid()) return resolve(null);
        resolve(session.getIdToken().getJwtToken());
      });
    });
  }

  /** Decodes claims from the current ID token (tenant_id, role, email). */
  function getClaims() {
    return new Promise((resolve) => {
      const user = pool().getCurrentUser();
      if (!user) return resolve(null);
      user.getSession((err, session) => {
        if (err || !session || !session.isValid()) return resolve(null);
        const payload = session.getIdToken().decodePayload();
        resolve({
          email:    payload["email"]            || "",
          tenantId: payload["custom:tenant_id"] || "",
          role:     payload["custom:role"]      || "client",
          isAdmin:  payload["custom:tenant_id"] === "*",
        });
      });
    });
  }

  function logout() {
    const user = pool().getCurrentUser();
    if (user) user.signOut();
    window.location.href = "login.html";
  }

  /**
   * Page guard. Call at the top of a protected page.
   * Redirects to login.html if no valid session.
   * Returns the claims object on success.
   */
  async function requireAuth() {
    const token = await getToken();
    if (!token) {
      window.location.href = "login.html";
      return null;
    }
    return getClaims();
  }

  return { login, completeNewPassword, getToken, getClaims, logout, requireAuth, COGNITO_CONFIG };
})();
