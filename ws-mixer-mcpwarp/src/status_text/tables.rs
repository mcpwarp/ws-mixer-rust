//! The six normative status-text tables (MCPWARP.md §7), string-identical to
//! `spec/fixtures/mcpwarp/status-text/<locale>.json`. Changing a string is a
//! spec release; `tests/fixtures_status_text.rs` fails on any drift.

pub(crate) struct Table {
    pub(crate) locale: &'static str,
    /// Between the `INVALID_CONFIG` sentence and `lastError` (§7 rule 7).
    pub(crate) joiner: &'static str,
    pub(crate) states: [(&'static str, &'static str); 6],
    pub(crate) codes: [(&'static str, &'static str); 13],
}

impl Table {
    pub(crate) fn state(&self, key: &str) -> Option<&'static str> {
        lookup(&self.states, key)
    }

    pub(crate) fn code(&self, key: &str) -> Option<&'static str> {
        lookup(&self.codes, key)
    }
}

fn lookup(entries: &[(&'static str, &'static str)], key: &str) -> Option<&'static str> {
    entries.iter().find(|(k, _)| *k == key).map(|(_, v)| *v)
}

/// In resolution order, the same as [`super::STATUS_LOCALES`].
pub(crate) static TABLES: [Table; 6] = [EN, DE, ES, FR, JA, ZH_CN];

const EN: Table = Table {
    locale: "en",
    joiner: ": ",
    states: [
        ("off", "Off"),
        ("connecting", "Connecting…"),
        ("online", "Online"),
        ("disabled", "Disabled in the mcpwarp dashboard"),
        ("conflict", "Another instance took over this name"),
        ("error", "Error"),
    ],
    codes: [
        ("UNAUTHORIZED", "Access token was rejected"),
        ("TAKEN_OVER", "Another instance took over this name"),
        ("DISCONNECTED", "Connection lost, reconnecting"),
        ("RETRYING", "Connection failed, retrying"),
        ("DISABLED", "Disabled in the mcpwarp dashboard"),
        ("INVALID_CONFIG", "Invalid settings"),
        (
            "INVALID_NAME",
            "Invalid service name (lowercase letters, digits and inner dashes, at most 30 characters)",
        ),
        (
            "USERNAME_REQUIRED",
            "Set a username in your mcpwarp account",
        ),
        ("QUOTA_EXCEEDED", "Plan limit reached"),
        ("CONFLICT", "Name already in use"),
        ("UNSUPPORTED_VERSION", "Plugin update required"),
        ("DISCONNECTED_FATAL", "Connection to mcpwarp failed"),
        ("INTERNAL", "Unexpected error"),
    ],
};

const DE: Table = Table {
    locale: "de",
    joiner: ": ",
    states: [
        ("off", "Aus"),
        ("connecting", "Verbinden…"),
        ("online", "Online"),
        ("disabled", "Im mcpwarp-Dashboard deaktiviert"),
        (
            "conflict",
            "Eine andere Instanz hat diesen Namen übernommen",
        ),
        ("error", "Fehler"),
    ],
    codes: [
        ("UNAUTHORIZED", "Zugriffstoken wurde abgelehnt"),
        (
            "TAKEN_OVER",
            "Eine andere Instanz hat diesen Namen übernommen",
        ),
        (
            "DISCONNECTED",
            "Verbindung unterbrochen, wird wiederhergestellt",
        ),
        ("RETRYING", "Verbindung fehlgeschlagen, neuer Versuch"),
        ("DISABLED", "Im mcpwarp-Dashboard deaktiviert"),
        ("INVALID_CONFIG", "Ungültige Einstellungen"),
        (
            "INVALID_NAME",
            "Ungültiger Dienstname (Kleinbuchstaben, Ziffern und innere Bindestriche, höchstens 30 Zeichen)",
        ),
        (
            "USERNAME_REQUIRED",
            "Legen Sie in Ihrem mcpwarp-Konto einen Benutzernamen fest",
        ),
        ("QUOTA_EXCEEDED", "Tariflimit erreicht"),
        ("CONFLICT", "Name wird bereits verwendet"),
        ("UNSUPPORTED_VERSION", "Plugin-Update erforderlich"),
        ("DISCONNECTED_FATAL", "Verbindung zu mcpwarp fehlgeschlagen"),
        ("INTERNAL", "Unerwarteter Fehler"),
    ],
};

const ES: Table = Table {
    locale: "es",
    joiner: ": ",
    states: [
        ("off", "Desactivado"),
        ("connecting", "Conectando…"),
        ("online", "En línea"),
        ("disabled", "Desactivado en el panel de mcpwarp"),
        ("conflict", "Otra instancia ha tomado este nombre"),
        ("error", "Error"),
    ],
    codes: [
        ("UNAUTHORIZED", "Token de acceso rechazado"),
        ("TAKEN_OVER", "Otra instancia ha tomado este nombre"),
        ("DISCONNECTED", "Conexión perdida, reconectando"),
        ("RETRYING", "Error de conexión, reintentando"),
        ("DISABLED", "Desactivado en el panel de mcpwarp"),
        ("INVALID_CONFIG", "Configuración no válida"),
        (
            "INVALID_NAME",
            "Nombre de servicio no válido (letras minúsculas, dígitos y guiones internos, 30 caracteres como máximo)",
        ),
        (
            "USERNAME_REQUIRED",
            "Define un nombre de usuario en tu cuenta de mcpwarp",
        ),
        ("QUOTA_EXCEEDED", "Límite del plan alcanzado"),
        ("CONFLICT", "El nombre ya está en uso"),
        ("UNSUPPORTED_VERSION", "Es necesario actualizar el plugin"),
        ("DISCONNECTED_FATAL", "No se pudo conectar con mcpwarp"),
        ("INTERNAL", "Error inesperado"),
    ],
};

const FR: Table = Table {
    locale: "fr",
    // U+00A0 NO-BREAK SPACE, colon, space.
    joiner: "\u{a0}: ",
    states: [
        ("off", "Désactivé"),
        ("connecting", "Connexion…"),
        ("online", "En ligne"),
        ("disabled", "Désactivé dans le tableau de bord mcpwarp"),
        ("conflict", "Une autre instance a repris ce nom"),
        ("error", "Erreur"),
    ],
    codes: [
        ("UNAUTHORIZED", "Jeton d'accès refusé"),
        ("TAKEN_OVER", "Une autre instance a repris ce nom"),
        ("DISCONNECTED", "Connexion perdue, reconnexion en cours"),
        ("RETRYING", "Échec de la connexion, nouvel essai en cours"),
        ("DISABLED", "Désactivé dans le tableau de bord mcpwarp"),
        ("INVALID_CONFIG", "Paramètres non valides"),
        (
            "INVALID_NAME",
            "Nom de service non valide (lettres minuscules, chiffres et tirets internes, 30 caractères au maximum)",
        ),
        (
            "USERNAME_REQUIRED",
            "Définissez un nom d'utilisateur dans votre compte mcpwarp",
        ),
        ("QUOTA_EXCEEDED", "Limite de l'offre atteinte"),
        ("CONFLICT", "Nom déjà utilisé"),
        ("UNSUPPORTED_VERSION", "Mise à jour du plugin requise"),
        ("DISCONNECTED_FATAL", "Échec de la connexion à mcpwarp"),
        ("INTERNAL", "Erreur inattendue"),
    ],
};

const JA: Table = Table {
    locale: "ja",
    // U+FF1A FULLWIDTH COLON.
    joiner: "\u{ff1a}",
    states: [
        ("off", "オフ"),
        ("connecting", "接続中…"),
        ("online", "オンライン"),
        ("disabled", "mcpwarp ダッシュボードで無効化されています"),
        ("conflict", "別のインスタンスがこの名前を取得しました"),
        ("error", "エラー"),
    ],
    codes: [
        ("UNAUTHORIZED", "アクセストークンが拒否されました"),
        ("TAKEN_OVER", "別のインスタンスがこの名前を取得しました"),
        ("DISCONNECTED", "接続が切れたため再接続しています"),
        ("RETRYING", "接続に失敗したため再試行しています"),
        ("DISABLED", "mcpwarp ダッシュボードで無効化されています"),
        ("INVALID_CONFIG", "設定が無効です"),
        (
            "INVALID_NAME",
            "サービス名が無効です（英小文字、数字、先頭と末尾以外のハイフン、30 文字以内）",
        ),
        (
            "USERNAME_REQUIRED",
            "mcpwarp アカウントでユーザー名を設定してください",
        ),
        ("QUOTA_EXCEEDED", "プランの上限に達しました"),
        ("CONFLICT", "この名前は既に使用されています"),
        ("UNSUPPORTED_VERSION", "プラグインの更新が必要です"),
        ("DISCONNECTED_FATAL", "mcpwarp への接続に失敗しました"),
        ("INTERNAL", "予期しないエラー"),
    ],
};

const ZH_CN: Table = Table {
    locale: "zh-CN",
    // U+FF1A FULLWIDTH COLON.
    joiner: "\u{ff1a}",
    states: [
        ("off", "已关闭"),
        ("connecting", "正在连接…"),
        ("online", "在线"),
        ("disabled", "已在 mcpwarp 控制台中停用"),
        ("conflict", "另一个实例已接管该名称"),
        ("error", "错误"),
    ],
    codes: [
        ("UNAUTHORIZED", "访问令牌被拒绝"),
        ("TAKEN_OVER", "另一个实例已接管该名称"),
        ("DISCONNECTED", "连接已断开，正在重新连接"),
        ("RETRYING", "连接失败，正在重试"),
        ("DISABLED", "已在 mcpwarp 控制台中停用"),
        ("INVALID_CONFIG", "设置无效"),
        (
            "INVALID_NAME",
            "服务名称无效（小写字母、数字和中间的连字符，最多 30 个字符）",
        ),
        ("USERNAME_REQUIRED", "请在 mcpwarp 账户中设置用户名"),
        ("QUOTA_EXCEEDED", "已达到套餐限额"),
        ("CONFLICT", "名称已被占用"),
        ("UNSUPPORTED_VERSION", "需要更新插件"),
        ("DISCONNECTED_FATAL", "无法连接到 mcpwarp"),
        ("INTERNAL", "意外错误"),
    ],
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TunnelState, TunnelStatusCode};

    #[test]
    fn every_table_has_every_state_and_code_in_spec_order() {
        for t in &TABLES {
            let states: Vec<_> = t.states.iter().map(|(k, _)| *k).collect();
            let want: Vec<_> = TunnelState::ALL.iter().map(|s| s.as_str()).collect();
            assert_eq!(states, want, "{}", t.locale);
            let codes: Vec<_> = t.codes.iter().map(|(k, _)| *k).collect();
            let want: Vec<_> = TunnelStatusCode::ALL.iter().map(|c| c.as_str()).collect();
            assert_eq!(codes, want, "{}", t.locale);
        }
    }

    /// rules.json `lint`.
    #[test]
    fn lint() {
        let en = &TABLES[0];
        for t in &TABLES {
            for (key, s) in t.states.iter().chain(&t.codes) {
                let at = format!("{} {key}", t.locale);
                assert!(!s.is_empty(), "{at}");
                assert!(!s.contains("..."), "{at}: ASCII '...'");
                assert!(!s.ends_with(['.', '。']), "{at}: trailing period");
                assert_eq!(
                    *s,
                    s.trim_matches(super::super::is_js_whitespace),
                    "{at}: surrounding whitespace"
                );
            }
            if t.locale == "en" {
                continue;
            }
            for (key, s) in t.states.iter() {
                let same = en.state(key) == Some(*s);
                let allowed =
                    (t.locale, *key) == ("de", "online") || (t.locale, *key) == ("es", "error");
                assert_eq!(same, allowed, "{} states.{key}", t.locale);
            }
            for (key, s) in t.codes.iter() {
                assert_ne!(en.code(key), Some(*s), "{} codes.{key}", t.locale);
            }
        }
    }
}
