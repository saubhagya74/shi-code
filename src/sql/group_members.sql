
create table group_members (
    group_id bigint not null references group_conversations(chat_id) on delete cascade,
    member_id bigint not null references users(user_id) on delete cascade,
    joined_at timestamptz default current_timestamp,
    is_admin boolean default false,
    -- Ensure a user can't be added to the same group twice
    primary key (group_id, member_id)
);

-- Index for the "Chat List": Find all groups a user belongs to
create index idx_group_members_user on group_members (member_id);
