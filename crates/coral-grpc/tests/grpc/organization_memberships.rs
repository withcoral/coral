use super::*;

/// coral-workspace and coral-administration own who may inspect and remove
/// members, the last Owner, and rejoin revisions
/// (`only_current_organization_owners_can_inspect_members_for_removal`,
/// `concurrent_administration_cannot_remove_the_last_enabled_owner`,
/// `organization_rejoin_tests.rs`). This checks the membership projection,
/// the removal RPC, and one denial.
#[tokio::test]
async fn organization_memberships_project_members_and_removal_needs_an_owner() {
    let f = Fixture::new().await;
    let mut client = f.organization_memberships();
    let owner = f.token(&f.owner, "reef");
    let member = f.token(&f.member, "reef");
    let own = client
        .get_current_organization_membership(request(
            management::GetCurrentOrganizationMembershipRequest::default(),
            &member,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(own.name, "organizationMemberships/member");
    assert_eq!(own.role, management::OrganizationRole::Member as i32);
    assert_eq!(own.profile.unwrap().user, "users/member");
    let list = client
        .list_organization_memberships(request(
            management::ListOrganizationMembershipsRequest::default(),
            &owner,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let target = list
        .memberships
        .iter()
        .find(|m| m.name == "organizationMemberships/member")
        .unwrap();
    // A present and an absent membership are refused alike.
    for name in [target.name.clone(), "organizationMemberships/absent".into()] {
        let error = client
            .delete_organization_membership(request(
                management::DeleteOrganizationMembershipRequest {
                    name: name.clone(),
                    etag: target.etag.clone(),
                    transfer_to: String::new(),
                },
                &member,
                "reef",
            ))
            .await
            .unwrap_err();
        assert_eq!(
            (error.code(), error_reason(&error).as_str()),
            (tonic::Code::PermissionDenied, "PERMISSION_DENIED"),
            "{name}"
        );
    }
    client
        .delete_organization_membership(request(
            management::DeleteOrganizationMembershipRequest {
                name: target.name.clone(),
                etag: target.etag.clone(),
                transfer_to: String::new(),
            },
            &owner,
            "reef",
        ))
        .await
        .unwrap();
    let remaining = client
        .list_organization_memberships(request(
            management::ListOrganizationMembershipsRequest::default(),
            &owner,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(
        remaining
            .memberships
            .iter()
            .all(|membership| membership.name != target.name)
    );
}

// Catches: a removal that answers the etag of the earlier form, a UUID, as a
// malformed one, so that Reef blames the form's fields when the page only
// needs a reload; and a refused removal that moves the Membership's etag.
#[tokio::test]
async fn removing_an_organization_membership_answers_a_stale_etag_as_a_mismatch() {
    let f = Fixture::new().await;
    let mut client = f.organization_memberships();
    let owner = f.token(&f.owner, "reef");
    let name = "organizationMemberships/member";
    let mut reader = f.organization_memberships();
    let mut etag = async || {
        reader
            .list_organization_memberships(request(
                management::ListOrganizationMembershipsRequest::default(),
                &owner,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner()
            .memberships
            .into_iter()
            .find(|membership| membership.name == name)
            .map(|membership| membership.etag)
    };
    assert_eq!(etag().await.as_deref(), Some("1"));
    let earlier_form = uuid::Uuid::new_v4().to_string();
    for (sent, expected) in [
        ("", (tonic::Code::InvalidArgument, "INVALID_ARGUMENT")),
        ("0", (tonic::Code::InvalidArgument, "INVALID_ARGUMENT")),
        ("stale", (tonic::Code::InvalidArgument, "INVALID_ARGUMENT")),
        ("2", (tonic::Code::Aborted, "ETAG_MISMATCH")),
        (
            earlier_form.as_str(),
            (tonic::Code::Aborted, "ETAG_MISMATCH"),
        ),
    ] {
        let error = client
            .delete_organization_membership(request(
                management::DeleteOrganizationMembershipRequest {
                    name: name.into(),
                    etag: sent.into(),
                    transfer_to: String::new(),
                },
                &owner,
                "reef",
            ))
            .await
            .unwrap_err();
        assert_eq!(
            (error.code(), error_reason(&error).as_str()),
            expected,
            "{sent:?}"
        );
        assert_eq!(etag().await.as_deref(), Some("1"), "{sent:?}");
    }
}
