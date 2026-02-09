-- 1. The Group Header
create table group_conversations (
    chat_id bigint primary key,
    group_name varchar(100) not null,
    creater_id bigint not null references users(user_id),
    
    last_message text,
    last_message_time timestamptz default current_timestamp,
    
    -- Using jsonb for performance and indexing
    settings jsonb default '{}'::jsonb,
    created_at timestamptz default current_timestamp
);


-- Index for sorting groups by recent activity
create index idx_group_activity on group_conversations (last_message_time desc);