
-- item_count : prod.integrations.order_items.id
-- id: prod.integrations.orders.id
select orders.id, COUNT(order_items.id) as item_count
from orders, integrations.order_items
where order_items.order_id = orders.id
group by order_items.order_id;

-- id: prod.integrations.orders.id
select id from orders;

-- order_id: prod.platform.order_items.count, prod.integrations.orders.price
select order_id from integrations.order_items
union all
select order_id from platform.order_items;

-- total_price: prod.platform.order_items.count, prod.integrations.orders.price
select sum(count * price) as total_price
from orders, platform.order_items
where order_items.order_id = orders.id;

-- total_price: prod.integrations.order_items.count, prod.platform.order_items.count, prod.integrations.orders.price
select sum(order_items.count * price) as total_price
from orders, (
  select order_id as my_order_id, count from integrations.order_items
  union all
  select order_id as my_order_id, count from platform.order_items
) order_items
where order_items.my_order_id = orders.id;
