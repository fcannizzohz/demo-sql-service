CREATE
OR REPLACE VIEW temperature_updates_ordered AS
SELECT *
FROM
    TABLE(
            IMPOSE_ORDER(
                TABLE temperature_updates, -- (1)
                DESCRIPTOR (ts), -- (2)
                INTERVAL '5' SECOND -- (3)
            )
    );