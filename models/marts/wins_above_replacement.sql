with bref_batting as (

    select
        player_year_id
      , person_id
      , year_id
      , sum(wins_above_replacement) as wins_above_replacement
    
    from {{ ref('stg_bref__batting_war') }}

    group by 1, 2, 3

),

bref_pitching as (

    select
        player_year_id
      , person_id
      , year_id
      , sum(wins_above_replacement) as wins_above_replacement
      
      from {{ ref('stg_bref__pitching_war') }}

      group by 1, 2, 3

),

fangraphs_batting as (

    select 
        player_year_id
      , person_id
      , year_id
      , wins_above_replacement 
      
    from {{ ref('stg_fangraphs__batting') }}

),

fangraphs_pitching as (

    select
        player_year_id
      , person_id
      , year_id
      , wins_above_replacement 
      
    from {{ ref('stg_fangraphs__pitching') }}

),

chadwick as (

    select * from {{ ref('util_chadwick__register') }}

),

spine as (

    select 
        player_year_id
      , person_id
      , year_id

    from bref_batting

    union distinct

    select
        player_year_id
      , person_id
      , year_id

    from bref_pitching

    union distinct

    select
        player_year_id
      , person_id
      , year_id

    from fangraphs_batting

    union distinct

    select
        player_year_id
      , person_id
      , year_id

    from fangraphs_pitching

),

final as (

    select
        spine.player_year_id
      , spine.person_id
      , spine.year_id
      , chadwick.last_name
      , chadwick.first_name
      , bref_batting.wins_above_replacement as br_batting_war
      , fangraphs_batting.wins_above_replacement as fg_batting_war
      , bref_pitching.wins_above_replacement as br_pitching_war
      , fangraphs_pitching.wins_above_replacement as fg_pitching_war
      , ifnull(bref_batting.wins_above_replacement, 0) +
        ifnull(bref_pitching.wins_above_replacement, 0) as br_war
      , ifnull(fangraphs_batting.wins_above_replacement, 0) +
        ifnull(fangraphs_pitching.wins_above_replacement, 0) as fg_war

    from spine
    inner join chadwick
        on spine.person_id = chadwick.person_id
    left join bref_batting
        on spine.player_year_id = bref_batting.player_year_id
    left join bref_pitching
        on spine.player_year_id = bref_pitching.player_year_id
    left join fangraphs_batting
        on spine.player_year_id = fangraphs_batting.player_year_id
    left join fangraphs_pitching
        on spine.player_year_id = fangraphs_pitching.player_year_id

)

select * from final