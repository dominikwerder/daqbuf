mod bins00;
pub mod bins_gen;
mod compare;
mod events00;
mod events01;

use super::container_events::ContainerEvents;
use std::any;

#[test]
fn test_use_serde() {
    let x = ContainerEvents::<f32>::new();
    let a: &dyn any::Any = &x;
    assert_eq!(a.downcast_ref::<String>().is_some(), false);
    assert_eq!(a.downcast_ref::<ContainerEvents<f32>>().is_some(), true);
    let s = serde_json::to_string(&x).unwrap();
    let _: ContainerEvents<f32> = serde_json::from_str(&s).unwrap();
}
