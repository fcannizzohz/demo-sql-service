CREATE
OR REPLACE VIEW temperature_updates_ordered AS
SELECT *
FROM
    TABLE(
            IMPOSE_ORDER(
                TABLE temperature_updates,
                DESCRIPTOR (ts),
                INTERVAL '5' SECOND
            )
    );