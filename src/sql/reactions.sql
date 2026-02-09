CREATE TABLE reactions_ (
    message_id_ bigint not null,
    user_id_ bigint not null references users(user_id),
    emoji_id_ varchar(10) not null,
    reacted_at timestamptz default current_timestamp,

    PRIMARY KEY (message_id_, user_id_)
);

CREATE INDEX idx_reactions_message_id ON reactions_ (message_id_);