create table topreactions_(
    message_id_ bigint primary key,
    top_emoji_ jsonb default '{}'::jsonb
)