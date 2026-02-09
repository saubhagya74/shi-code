create type request_status_type AS ENUM ('pending', 'accepted', 'declined');

--friendship/follow req table
CREATE TABLE request_ (
    request_id_ bigint primary key,
    sender_id_ bigint not null references users(user_id),
    receiver_id_ bigint not null references users(user_id),
    status request_status_type not null default 'pending',
    requested_at timestamptz default current_timestamp,
    
    CONSTRAINT unique_request_pair UNIQUE (sender_id_, receiver_id_),
    --prevent from sending to self
    CONSTRAINT check_not_self CHECK (sender_id_ <> receiver_id_)
);

