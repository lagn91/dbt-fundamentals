with 

orders as (

    select * from {{ ref('stg_jaffle_shop_orders') }}

),

customers as (

    select * from {{ ref('stg_jaffle_shop_customers') }}

),

payments as (

    select * from {{ ref('stg_stripe_payments') }}

),

completed_payments as (

    select 
        order_id, 
        max(payment_created_at) as payment_finalized_date, 
        sum(payment_amount) as total_amount_paid
    from payments
    where payment_status <> 'fail'
    group by 1

),

paid_orders as (

    select 
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,

        completed_payments.total_amount_paid,
        completed_payments.payment_finalized_date,

        customers.customer_first_name,
        customers.customer_last_name
    from orders
    left join completed_payments on orders.order_id = completed_payments.order_id
    left join customers on orders.customer_id = customers.customer_id 

),

final as (
    select
        order_id,
        customer_id,
        order_placed_at,
        order_status,
        total_amount_paid,
        payment_finalized_date,
        customer_first_name,
        customer_last_name,

        row_number() over (
            order by order_id) as transaction_seq,
        row_number() over (
            partition by customer_id 
            order by order_id) as customer_sales_seq,
        case 
            when  (
            rank () over (
                partition by customer_id 
                order by order_placed_at, order_id
                ) = 1
            ) then 'new'
        else 'return' end as nvsr,

        sum(total_amount_paid) over (
            partition by customer_id
            order by order_placed_at
            ) as customer_lifetime_value,

        first_value(paid_orders.order_placed_at) over (
            partition by paid_orders.customer_id
            order by paid_orders.order_placed_at
         ) as fdos

    from paid_orders
)

select * from final
order by order_id
