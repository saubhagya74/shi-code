create table stories (
    story_id bigint primary key,
    user_id bigint not null,
    content_url text not null,
    view_count int default 0,
    like_count int default 0,
    comment_count int default 0,
    created_at timestamptz default current_timestamp,
); --put alive sotry count as well
create index idx_stories_user_id on stories(user_id, created_at desc);