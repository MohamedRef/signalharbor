use signalharbor::{HarborError, RetryPolicy, plan_retries};

fn policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 5,
        base_delay_seconds: 10,
        max_delay_seconds: 60,
        max_elapsed_seconds: 300,
        jitter_basis_points: 0,
        jitter_seed: 42,
    }
}

#[test]
fn exponential_schedule_caps_delays_and_accumulates_due_times() {
    let plan = plan_retries(1_000, policy()).unwrap();
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| (slot.attempt, slot.delay_seconds, slot.due_at))
            .collect::<Vec<_>>(),
        [
            (1, 10, 1_010),
            (2, 20, 1_030),
            (3, 40, 1_070),
            (4, 60, 1_130),
            (5, 60, 1_190),
        ]
    );
    assert!(!plan.exhausted_by_elapsed_limit);
}

#[test]
fn elapsed_budget_stops_before_an_out_of_budget_attempt() {
    let plan = plan_retries(
        100,
        RetryPolicy {
            max_elapsed_seconds: 65,
            ..policy()
        },
    )
    .unwrap();
    assert_eq!(plan.slots.len(), 2);
    assert_eq!(plan.slots[1].due_at, 130);
    assert!(plan.exhausted_by_elapsed_limit);
}

#[test]
fn seeded_jitter_is_repeatable_and_stays_inside_bounds() {
    let jittered = RetryPolicy {
        jitter_basis_points: 2_500,
        ..policy()
    };
    let first = plan_retries(0, jittered).unwrap();
    let second = plan_retries(0, jittered).unwrap();
    assert_eq!(first, second);
    for (slot, nominal) in first.slots.iter().zip([10_u64, 20, 40, 60, 60]) {
        assert!(slot.delay_seconds >= nominal * 3 / 4);
        assert!(slot.delay_seconds <= nominal * 5 / 4);
    }
}

#[test]
fn retry_policy_rejects_invalid_bounds_and_overflow() {
    assert!(matches!(
        plan_retries(
            0,
            RetryPolicy {
                max_attempts: 0,
                ..policy()
            }
        ),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        plan_retries(
            0,
            RetryPolicy {
                max_delay_seconds: 5,
                ..policy()
            }
        ),
        Err(HarborError::Validation(_))
    ));
    assert!(matches!(
        plan_retries(u64::MAX, policy()),
        Err(HarborError::Validation(_))
    ));
}
