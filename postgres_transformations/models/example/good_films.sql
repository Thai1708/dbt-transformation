SELECT
    film_id,
    title,
    user_rating::varchar(100) as user_rating,
    release_date
FROM {{ ref('films') }}
WHERE user_rating > 4.5