create table followed_following(
    follower_id bigint not null, --user id; followed by
    following_id bigint not null, --followed to?
    followed_at timestamptz default CURRENT_TIMESTAMP,
    is_followed_to_heavy boolean,
    is_active    boolean DEFAULT true,
    primary key (follower_id, following_id)
)
create index idx_get_followers on followed_following (following_id);

create table story_notifications (
    user_id bigint not null,           -- person seeing the story
    story_id bigint not null,            -- story 
    creator_id bigint not null,        -- the one who posted
    story_created_at timestamptz not null,
    --prevents duplicate notification
    primary key (user_id, story_id)
);
create index idx_get_story on story_notifications (user_id, story_created_at desc);

create table stories (
    story_id bigint primary key,
    user_id bigint not null,
    content_url text not null,
    view_count int default 0,
    like_count int default 0,
    comment_count int default 0,
    created_at timestamptz default current_timestamp
);
create index idx_stories_user_id on stories(user_id, created_at desc);
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