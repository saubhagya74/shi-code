create table conversations (
    chat_id uuid primary key default gen_random_uuid(),
    chat_name varchar(100), -- can be null

    user_a_id bigint not null references users(user_id),
    user_b_id bigint not null references users(user_id),
    --user_a_id bigint not null,
    --user_b_id bigint not null,

    last_message text,
    last_message_time timestamptz default current_timestamp,

    view_a_time timestamptz default current_timestamp, --for limiting user a 
    view_b_time timestamptz default current_timestamp, --for limiting user b
    
    settings jsonb default '{}'::jsonb,
    
    constraint check_user_order check (user_a_id > user_b_id)
);

create index idx_conv_user_a on conversations (user_a_id, last_message_time desc); --for fetching recent talks
create index idx_conv_user_b on conversations (user_b_id, last_message_time desc);

create unique index idx_unique_pair on conversations (user_a_id, user_b_id); --for putting in memory whcih will be used for messaging