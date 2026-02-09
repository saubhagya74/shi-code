create table story_notifications (
    user_id bigint not null,           -- person seeing the story
    story_id uuid not null,            -- story 
    creator_id bigint not null,        -- the one who posted
    story_created_at timestamptz not null,
    --prevents duplicate notification
    primary key (user_id, story_id)
);
create index idx_get_story on story_notifications (user_id, story_created_at desc);