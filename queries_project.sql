-- For any reason the tables do not take the reference system EPSG 4326
SELECT ST_SRID(geometry) FROM mines limit 1;

-- Number of mines within 50 KM of each iron mine 
select m."SITE_NAME", mi."COMMODITY", mm."SITE_NAME", mr."COMMODITY" , ST_Distance(ST_transform(ST_SetSRID(m."geometry",4326),26918) , ST_transform(ST_SetSRID(mm."geometry",4326),26918)) as DISTANCE_IN_METERS
from mines m , minerals mi, mines mm, minerals mr
where mi."COMMODITY" = 'Iron' and (m."id_mine" = mi."id_mine") 
and ST_Distance(ST_transform(ST_SetSRID(m."geometry",4326),26918) , ST_transform(ST_SetSRID(mm."geometry",4326),26918)) < 50000 
and m."id_mine" <> mm."id_mine" and mr."id_mine" = mm."id_mine" 
and mr."COMMODITY" <> 'Iron'
order by m."SITE_NAME", DISTANCE_IN_METERS

-- Number of mines within 50 KM of each gold mine 
select m."SITE_NAME", mi."COMMODITY", mm."SITE_NAME", mr."COMMODITY" , ST_Distance(ST_transform(ST_SetSRID(m."geometry",4326),26918) , ST_transform(ST_SetSRID(mm."geometry",4326),26918)) as DISTANCE_IN_METERS
from mines m , minerals mi, mines mm, minerals mr
where mi."COMMODITY" = 'Gold' and (m."id_mine" = mi."id_mine") 
and ST_Distance(ST_transform(ST_SetSRID(m."geometry",4326),26918) , ST_transform(ST_SetSRID(mm."geometry",4326),26918)) < 50000 
and m."id_mine" <> mm."id_mine" and mr."id_mine" = mm."id_mine" 
and mr."COMMODITY" <> 'Gold'
order by m."SITE_NAME", DISTANCE_IN_METERS


-- Number of mines within 50 KM of each copper mine 
select m."SITE_NAME", mi."COMMODITY", mm."SITE_NAME", mr."COMMODITY" , ST_Distance(ST_transform(ST_SetSRID(m."geometry",4326),26918) , ST_transform(ST_SetSRID(mm."geometry",4326),26918)) as DISTANCE_IN_METERS 
from mines m , minerals mi, mines mm, minerals mr
where mi."COMMODITY" = 'Copper' and (m."id_mine" = mi."id_mine") 
and ST_Distance(ST_transform(ST_SetSRID(m."geometry",4326),26918) , ST_transform(ST_SetSRID(mm."geometry",4326),26918)) < 50000 
and m."id_mine" <> mm."id_mine" and mr."id_mine" = mm."id_mine" 
and mr."COMMODITY" <> 'Copper'
order by m."SITE_NAME", DISTANCE_IN_METERS

-- Mines per State
SELECT "STATE_LOCA", count ("id_mine")
from states
Group by "STATE_LOCA" 

-- Iron Mines per State
SELECT s."STATE_LOCA", m."COMMODITY", count (s."id_mine")
from states s, minerals m
where m."COMMODITY" = 'Iron' and s."id_mine" = m."id_mine"
Group by s."STATE_LOCA", m."COMMODITY"
order by s."STATE_LOCA", m."COMMODITY"

-- Gold Mines per State
SELECT s."STATE_LOCA", m."COMMODITY", count (s."id_mine")
from states s, minerals m
where m."COMMODITY" = 'Gold' and s."id_mine" = m."id_mine"
Group by s."STATE_LOCA", m."COMMODITY"
order by s."STATE_LOCA", m."COMMODITY"

-- Copper Mines density per State
SELECT s."STATE_LOCA", s."area_sq_km", m."COMMODITY", count (s."id_mine"), (count(s."id_mine") / s."area_sq_km") as density_of_mines
from states s, minerals m
where m."COMMODITY" = 'Copper' and s."id_mine" = m."id_mine"
Group by s."STATE_LOCA", m."COMMODITY",s."area_sq_km"

-- Gold Mines density per State
SELECT s."STATE_LOCA", s."area_sq_km", m."COMMODITY", count (s."id_mine"), (count(s."id_mine") / s."area_sq_km") as density_of_mines
from states s, minerals m
where m."COMMODITY" = 'Gold' and s."id_mine" = m."id_mine"
Group by s."STATE_LOCA", m."COMMODITY",s."area_sq_km" 

-- Iron Mines density per State
SELECT s."STATE_LOCA", s."area_sq_km", m."COMMODITY", count (s."id_mine"), (count(s."id_mine") / s."area_sq_km") as density_of_mines
from states s, minerals m
where m."COMMODITY" = 'Iron' and s."id_mine" = m."id_mine"
Group by s."STATE_LOCA", m."COMMODITY",s."area_sq_km" 




