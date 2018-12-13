Set @Last30Days= date_add(@StartDate ,interval -30 day);
 
-- Select @StartDate,@EndDate,@Last30Days;
 
drop temporary table if exists tempdb.DG2_agent_30Days;
create temporary table tempdb.DG2_agent_30Days
select distinct agentid from bskybsatmap.`bskybinternal.skydg2` 
where saledt >= @Last30Days and saledt <= @EndDate;
 
Alter table tempdb.DG2_agent_30Days add index agentid(agentid);
 
drop temporary table if exists tempdb.Missing_agent_30Days_300calls;
create temporary table tempdb.Missing_agent_30Days_300calls
 select area, count(*) AS CountZeroAgent300 from (
select 
        area,
        dg1.agentid as agentid,
        count(*) count-- , sum(on_off) as sum
    from bskybsatmap.`bskybinternal.skysm1` dg1 left join tempdb.DG2_agent_30Days dg2
    on dg1.agentid=dg2.agentid
        where dg1.callstarttime >= @Last30Days and dg1.callstarttime <= @EndDate
        and dg2.agentid is null
        and skillgroup not  like 'TestAG_SER_%'  and skillgroup not  like '%concierge%' and skillgroup not  like '%none%' and on_off is not null
    group by area, dg1.agentid
    having count(*)>300 
   ) miss_Agent group by area;
 
   insert into bskybsatmap.`etl.cc_sensors_data_staging`(Sensors_Date,ProgramID,CustomGroup1,CustomGroup2,Sensors_Name,Sensors_Value_Num)
         select 
         @StartDate as Sensors_Date,
         'bskybinternal' as Programid,
         area ,
         '' as CustomGroup2,
         'Zero_Agents_Count_300' as Sensor_Name,
         CountZeroAgent300 as Sensors_Value_Num
        from tempdb.Missing_agent_30Days_300calls
        ON duplicate key update Sensors_Value_Num = CountZeroAgent300;
 
 
drop temporary table if exists tempdb.Missing_agent_30Days_50calls;
create temporary table tempdb.Missing_agent_30Days_50calls
 select area, count(*) AS CountZeroAgent50 from (
select 
        area,
        dg1.agentid as agentid,
        count(*) count-- , sum(on_off) as sum
    from bskybsatmap.`bskybinternal.skysm1` dg1 left join tempdb.DG2_agent_30Days dg2
    on dg1.agentid=dg2.agentid
        where dg1.callstarttime >= @Last30Days and dg1.callstarttime <= @EndDate
        and dg2.agentid is null
        and skillgroup not  like 'TestAG_SER_%'  and skillgroup not  like '%concierge%' and skillgroup not  like '%none%' and on_off is not null
    group by area, dg1.agentid
    having count(*)>50
   ) miss_Agent group by area;
 
 
 
    insert into bskybsatmap.`etl.cc_sensors_data_staging`(Sensors_Date,ProgramID,CustomGroup1,CustomGroup2,Sensors_Name,Sensors_Value_Num)
         select 
         @StartDate as Sensors_Date,
         'bskybinternal' as Programid,
         area ,
         '' as CustomGroup2,
         'Zero_Agents_Count' as Sensor_Name,
         CountZeroAgent50 as Sensors_Value_Num
        from tempdb.Missing_agent_30Days_50calls
        ON duplicate key update Sensors_Value_Num = CountZeroAgent50;