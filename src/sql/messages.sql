
create type content_label AS ENUM ('text', 'video', 'audio', 'image', 'file');
create type encryption_type AS ENUM ('none', 'rsa', 'ecc');
create type compression_type AS ENUM ('none', 'lz4', 'gzip', 'zstd');

create table messages (
    message_id bigserial primary key,
    chat_id bigint not null references conversations(chat_id) on delete cascade,
    sender_id bigint not null references users(user_id),
    receiver_id bigint not null references users(user_id),
    
    content_type content_label not null default 'text',
    description varchar(3000),
    
    encryption_type encryption_type default 'none',
    compression_type compression_type default 'none',
    
    reaction_id varchar(10), --emoji id
    is_edited boolean default false,
    is_deleted boolean default false,
    messaged_at timestamptz default current_timestamp
);

create index idx_messages_chat_time ON messages (chat_id, messaged_at DESC);