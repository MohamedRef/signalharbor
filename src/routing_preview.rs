use crate::{HarborError, Route};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteMatch {
    pub route_id: String,
    pub topic_pattern: String,
    pub specificity: usize,
    pub exact: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutePreview {
    pub tenant: String,
    pub topic: String,
    pub selected_route_id: Option<String>,
    pub matches: Vec<RouteMatch>,
    pub ambiguous: bool,
}

/// Resolves all active matches using exact routes before the longest wildcard prefix.
///
/// # Errors
///
/// Returns validation when tenant or topic is empty or exceeds routing limits.
pub fn preview_routes(
    routes: &[Route],
    tenant: &str,
    topic: &str,
) -> Result<RoutePreview, HarborError> {
    let tenant = normalized(tenant, "tenant", 80)?;
    let topic = normalized(topic, "topic", 120)?;
    let mut matches = routes
        .iter()
        .filter(|route| route.matches(&tenant, &topic))
        .map(|route| {
            let exact = !route.topic_pattern.ends_with('*');
            let specificity = route.topic_pattern.trim_end_matches('*').len();
            RouteMatch {
                route_id: route.id.clone(),
                topic_pattern: route.topic_pattern.clone(),
                specificity,
                exact,
            }
        })
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        right
            .exact
            .cmp(&left.exact)
            .then_with(|| right.specificity.cmp(&left.specificity))
            .then_with(|| left.route_id.cmp(&right.route_id))
    });
    let ambiguous = matches.first().is_some_and(|first| {
        matches.get(1).is_some_and(|second| {
            first.exact == second.exact && first.specificity == second.specificity
        })
    });
    Ok(RoutePreview {
        tenant,
        topic,
        selected_route_id: (!ambiguous)
            .then(|| matches.first().map(|item| item.route_id.clone()))
            .flatten(),
        matches,
        ambiguous,
    })
}

fn normalized(value: &str, field: &str, maximum: usize) -> Result<String, HarborError> {
    let value = value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    if value.is_empty() || value.len() > maximum {
        return Err(HarborError::Validation(format!(
            "{field} must contain 1 through {maximum} characters"
        )));
    }
    Ok(value)
}
