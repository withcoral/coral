//! Private application-secret references. Client settings belong to Provider configuration.
use crate::{Error, ProviderId, ProviderSetupId};
use coral_db::Transaction;
use coral_workspace::WorkspaceId;
use sea_query::{Alias, Expr, ExprTrait as _, Iden, Query};

#[derive(Iden)]
enum ProviderOauthClientSecrets {
    Table,
    Id,
    WorkspaceId,
    BoundClientId,
    EncryptedSecret,
}

pub(crate) enum Target<'a> {
    Setup(&'a ProviderSetupId),
    Provider(&'a ProviderId),
}

impl Target<'_> {
    fn table(&self) -> Alias {
        Alias::new(match self {
            Self::Setup(_) => "provider_setups",
            Self::Provider(_) => "provider_records",
        })
    }

    fn id(&self) -> String {
        match self {
            Self::Setup(id) => id.to_string(),
            Self::Provider(id) => id.to_string(),
        }
    }
}

#[derive(sqlx::FromRow)]
pub(crate) struct StoredSecret {
    pub(crate) id: String,
    /// Retention fence: changing the configured client ID requires a new secret.
    pub(crate) bound_client_id: String,
    pub(crate) encrypted_secret: String,
}

#[derive(Iden)]
enum ConfigurationRecords {
    Id,
    WorkspaceId,
    OauthClientSecretId,
}

fn configuration_scope(owner: &WorkspaceId, target: &Target<'_>) -> sea_query::SimpleExpr {
    let workspace = Expr::col(ConfigurationRecords::WorkspaceId).eq(owner.to_string());
    workspace.and(Expr::col(ConfigurationRecords::Id).eq(target.id()))
}

fn secret_scope(owner: &WorkspaceId, id: &str) -> sea_query::SimpleExpr {
    let workspace = Expr::col(ProviderOauthClientSecrets::WorkspaceId).eq(owner.to_string());
    workspace.and(Expr::col(ProviderOauthClientSecrets::Id).eq(id))
}

async fn reference(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    target: &Target<'_>,
) -> Result<Option<String>, Error> {
    let row: Option<(Option<String>,)> = tx
        .fetch_optional(
            Query::select()
                .column(ConfigurationRecords::OauthClientSecretId)
                .from(target.table())
                .and_where(configuration_scope(owner, target))
                .to_owned(),
        )
        .await?;
    Ok(row.and_then(|(id,)| id))
}

async fn set_reference(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    target: &Target<'_>,
    secret: Option<&str>,
) -> Result<(), Error> {
    let changed = tx
        .execute(
            Query::update()
                .table(target.table())
                .value(ConfigurationRecords::OauthClientSecretId, secret)
                .and_where(configuration_scope(owner, target))
                .to_owned(),
        )
        .await?;
    if changed != 1 {
        return Err(match target {
            Target::Setup(_) => Error::ProviderSetupNotFound,
            Target::Provider(_) => Error::ProviderNotFound,
        });
    }
    Ok(())
}

pub(crate) async fn load(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    target: &Target<'_>,
) -> Result<Option<StoredSecret>, Error> {
    let Some(id) = reference(tx, owner, target).await? else {
        return Ok(None);
    };
    Ok(tx
        .fetch_optional(
            Query::select()
                .columns([
                    ProviderOauthClientSecrets::Id,
                    ProviderOauthClientSecrets::BoundClientId,
                    ProviderOauthClientSecrets::EncryptedSecret,
                ])
                .from(ProviderOauthClientSecrets::Table)
                .and_where(secret_scope(owner, &id))
                .to_owned(),
        )
        .await?)
}

pub(crate) async fn save(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    target: &Target<'_>,
    secret: &StoredSecret,
) -> Result<(), Error> {
    delete(tx, owner, target).await?;
    tx.execute(
        Query::insert()
            .into_table(ProviderOauthClientSecrets::Table)
            .columns([
                ProviderOauthClientSecrets::Id,
                ProviderOauthClientSecrets::WorkspaceId,
                ProviderOauthClientSecrets::BoundClientId,
                ProviderOauthClientSecrets::EncryptedSecret,
            ])
            .values_panic([
                secret.id.clone().into(),
                Some(ToString::to_string(owner)).into(),
                secret.bound_client_id.clone().into(),
                secret.encrypted_secret.clone().into(),
            ])
            .to_owned(),
    )
    .await?;
    set_reference(tx, owner, target, Some(&secret.id)).await
}

pub(crate) async fn delete(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    target: &Target<'_>,
) -> Result<(), Error> {
    if let Some(id) = reference(tx, owner, target).await? {
        set_reference(tx, owner, target, None).await?;
        tx.execute(
            Query::delete()
                .from_table(ProviderOauthClientSecrets::Table)
                .and_where(secret_scope(owner, &id))
                .to_owned(),
        )
        .await?;
    }
    Ok(())
}

/// Transfers the Setup's secret reference and reports whether it replaced the Provider's secret.
pub(crate) async fn promote(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    setup: &ProviderSetupId,
    provider: &ProviderId,
) -> Result<bool, Error> {
    if let Some(id) = reference(tx, owner, &Target::Setup(setup)).await? {
        set_reference(tx, owner, &Target::Setup(setup), None).await?;
        delete(tx, owner, &Target::Provider(provider)).await?;
        set_reference(tx, owner, &Target::Provider(provider), Some(&id)).await?;
        return Ok(true);
    }
    Ok(false)
}

/// Reuses an Update target's reference without sharing ownership of its secret.
pub(crate) async fn for_setup(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    setup: &ProviderSetupId,
) -> Result<Option<StoredSecret>, Error> {
    if let Some(secret) = load(tx, owner, &Target::Setup(setup)).await? {
        return Ok(Some(secret));
    }
    let loaded = crate::repository::setup(&mut *tx, owner, setup)
        .await?
        .ok_or(Error::ProviderSetupNotFound)?;
    match loaded.update_provider_id {
        Some(provider) => load(tx, owner, &Target::Provider(&provider)).await,
        None => Ok(None),
    }
}
