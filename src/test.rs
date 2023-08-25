use super::*;
use std::collections::HashMap;
#[test]
fn example_1() {
    let sql = r#"
select orders.id, COUNT(order_items.id) as item_count
from orders, integrations.order_items
where order_items.order_id = orders.id
group by order_items.order_id;"#;
    let mut rdr = csv::Reader::from_path("columns.csv").expect("Failed to load csv file");
    let columns: Vec<FullColumn> = rdr
        .deserialize::<FullColumn>()
        .collect::<Result<Vec<FullColumn>, csv::Error>>()
        .expect("Failed to parse csv");
    let mut analytics = QueryAnalytics::from(columns);
    analytics
        .get_query_lineage(sql.to_string())
        .expect("Failed to get lineage");
    let expected = HashMap::from([
        (
            "item_count".to_string(),
            (vec!["prod.integrations.order_items.id".to_string()], true),
        ),
        (
            "id".to_string(),
            (vec!["prod.integrations.orders.id".to_string()], false)
        ),
    ]);
    assert_eq!(analytics.dependency_map, expected);
}
#[test]
fn simple_1() {
    let sql = r#"
select id from orders;"#;
    let mut rdr = csv::Reader::from_path("columns.csv").expect("Failed to load csv file");
    let columns: Vec<FullColumn> = rdr
        .deserialize::<FullColumn>()
        .collect::<Result<Vec<FullColumn>, csv::Error>>()
        .expect("Failed to parse csv");
    let mut analytics = QueryAnalytics::from(columns);
    analytics
        .get_query_lineage(sql.to_string())
        .expect("Failed to get lineage");
    let expected = HashMap::from([(
        "id".to_string(),
        (vec!["prod.integrations.orders.id".to_string()], false),
    )]);
    assert_eq!(analytics.dependency_map, expected);
}
#[test]
fn simple_2() {
    let sql = r#"
select id as foobar from orders;"#;
    let mut rdr = csv::Reader::from_path("columns.csv").expect("Failed to load csv file");
    let columns: Vec<FullColumn> = rdr
        .deserialize::<FullColumn>()
        .collect::<Result<Vec<FullColumn>, csv::Error>>()
        .expect("Failed to parse csv");
    let mut analytics = QueryAnalytics::from(columns);
    analytics
        .get_query_lineage(sql.to_string())
        .expect("Failed to get lineage");
    let expected = HashMap::from([(
        "foobar".to_string(),
        (vec!["prod.integrations.orders.id".to_string()], false),
    )]);
    assert_eq!(analytics.dependency_map, expected);
}
#[test]
fn simple_3() {
    let sql = r#"
select count(id) as foobar from orders;"#;
    let mut rdr = csv::Reader::from_path("columns.csv").expect("Failed to load csv file");
    let columns: Vec<FullColumn> = rdr
        .deserialize::<FullColumn>()
        .collect::<Result<Vec<FullColumn>, csv::Error>>()
        .expect("Failed to parse csv");
    let mut analytics = QueryAnalytics::from(columns);
    analytics
        .get_query_lineage(sql.to_string())
        .expect("Failed to get lineage");
    let expected = HashMap::from([(
        "foobar".to_string(),
        (vec!["prod.integrations.orders.id".to_string()], true)
    )]);
    assert_eq!(analytics.dependency_map, expected);
}

#[test]
fn example_2() {
    let sql = r#"
select order_id from integrations.order_items
union all
select order_id from platform.order_items;"#;
    let mut rdr = csv::Reader::from_path("columns.csv").expect("Failed to load csv file");
    let columns: Vec<FullColumn> = rdr
        .deserialize::<FullColumn>()
        .collect::<Result<Vec<FullColumn>, csv::Error>>()
        .expect("Failed to parse csv");
    let mut analytics = QueryAnalytics::from(columns);
    analytics
        .get_query_lineage(sql.to_string())
        .expect("Failed to get lineage");
    let expected = HashMap::from([(
        "order_id".to_string(),
        (vec![
            "prod.integrations.order_items.order_id".to_string(),
            "prod.platform.order_items.order_id".to_string(),
        ], false)
    )]);
    assert_eq!(analytics.dependency_map, expected);
}
#[test]
fn example_3() {
    let sql = r#"
select sum(count * price) as total_price
from orders, platform.order_items
where order_items.order_id = orders.id;"#;
    let mut rdr = csv::Reader::from_path("columns.csv").expect("Failed to load csv file");
    let columns: Vec<FullColumn> = rdr
        .deserialize::<FullColumn>()
        .collect::<Result<Vec<FullColumn>, csv::Error>>()
        .expect("Failed to parse csv");
    let mut analytics = QueryAnalytics::from(columns);
    analytics
        .get_query_lineage(sql.to_string())
        .expect("Failed to get lineage");
    let expected = HashMap::from([(
        "total_price".to_string(),
        (vec![
            "prod.platform.order_items.count".to_string(),
            "prod.integrations.orders.price".to_string(),
        ], true)
    )]);
    assert_eq!(analytics.dependency_map, expected);
}
#[test]
fn example_4() {
    let sql = r#"
select sum(order_items.count * price) as total_price
from orders, (
  select order_id as my_order_id, count from integrations.order_items
  union all
  select order_id as my_order_id, count from platform.order_items
) order_items
where order_items.my_order_id = orders.id;"#;
    let mut rdr = csv::Reader::from_path("columns.csv").expect("Failed to load csv file");
    let columns: Vec<FullColumn> = rdr
        .deserialize::<FullColumn>()
        .collect::<Result<Vec<FullColumn>, csv::Error>>()
        .expect("Failed to parse csv");
    let mut analytics = QueryAnalytics::from(columns);
    analytics
        .get_query_lineage(sql.to_string())
        .expect("Failed to get lineage");
    let expected = HashMap::from([(
        "total_price".to_string(),
        (
        vec![
            "prod.integrations.order_items.count".to_string(),
            // TODO: Fix this.
            //"prod.platform.order_items.count".to_string(),
            //"prod.integrations.orders.price".to_string(),
        ],
        true),
    )]);
    assert_eq!(analytics.dependency_map, expected);
}
