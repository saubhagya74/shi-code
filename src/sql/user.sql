create table users (
    user_id bigint primary key,
    username varchar(50) unique not null,
    display_name varchar(100) not null,
    email varchar(255) unique not null,
    pass_hash text not null,
    follower_count int default 0,
    following_count int default 0,
    priority_count int default 0,
    profile_url text,
    last_online timestamptz default current_timestamp,
    created_at timestamptz default current_timestamp
);
--create index idx_users_username on users(username); --not needed now redis will have userid: username