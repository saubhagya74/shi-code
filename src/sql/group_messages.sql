create table group_messages (
    message_id bigserial primary key,
    chat_id bigint not null references group_conversations(chat_id) on delete cascade,
    sender_id bigint not null references users(user_id),
    
    content_type content_label not null default 'text',
    description varchar(3000),
    
    encryption_type encryption_type default 'none',
    compression_type compression_type default 'none',
    
    reaction_id bigint, -- a seperate reaction table
    is_edited boolean default false,
    is_deleted boolean default false,
    messaged_at timestamptz default current_timestamp
);

create index idx_group_messages_chat_time ON group_messages (chat_id, messaged_at DESC);