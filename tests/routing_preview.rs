use signalharbor::{HarborError, Route, preview_routes};

fn route(id: &str, pattern: &str) -> Route {
    Route::new(id, "Tenant-A", pattern, "https://hooks.example.test", 5, 10).unwrap()
}

#[test]
fn exact_route_wins_over_matching_wildcards() {
    let routes = vec![
        route("broad", "invoice.*"),
        route("exact", "invoice.paid"),
        route("narrow", "invoice.paid.*"),
    ];
    let preview = preview_routes(&routes, " TENANT-A ", "Invoice.Paid").unwrap();
    assert_eq!(preview.selected_route_id.as_deref(), Some("exact"));
    assert_eq!(preview.matches.len(), 2);
    assert_eq!(preview.matches[0].route_id, "exact");
    assert!(!preview.ambiguous);
}

#[test]
fn longest_wildcard_prefix_wins_deterministically() {
    let routes = vec![
        route("broad", "invoice.*"),
        route("narrow", "invoice.paid.*"),
    ];
    let preview = preview_routes(&routes, "tenant-a", "invoice.paid.eu").unwrap();
    assert_eq!(preview.selected_route_id.as_deref(), Some("narrow"));
    assert_eq!(
        preview
            .matches
            .iter()
            .map(|item| item.route_id.as_str())
            .collect::<Vec<_>>(),
        ["narrow", "broad"]
    );
}

#[test]
fn equally_specific_routes_are_reported_as_ambiguous() {
    let routes = vec![route("second", "invoice.*"), route("first", "invoice.*")];
    let preview = preview_routes(&routes, "tenant-a", "invoice.created").unwrap();
    assert!(preview.ambiguous);
    assert_eq!(preview.selected_route_id, None);
    assert_eq!(preview.matches[0].route_id, "first");
}

#[test]
fn inactive_and_other_tenant_routes_are_ignored() {
    let mut inactive = route("inactive", "invoice.*");
    inactive.active = false;
    let other = Route::new(
        "other",
        "tenant-b",
        "invoice.*",
        "https://hooks.example.test",
        5,
        10,
    )
    .unwrap();
    let preview = preview_routes(&[inactive, other], "tenant-a", "invoice.created").unwrap();
    assert!(preview.matches.is_empty());
    assert_eq!(preview.selected_route_id, None);
}

#[test]
fn preview_validates_routing_identity() {
    assert!(matches!(
        preview_routes(&[], "", "topic"),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        preview_routes(&[], "tenant", ""),
        Err(HarborError::Validation(_))
    ));
}
