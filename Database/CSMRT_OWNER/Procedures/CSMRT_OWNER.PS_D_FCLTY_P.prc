DROP PROCEDURE CSMRT_OWNER.PS_D_FCLTY_P
/

--
-- PS_D_FCLTY_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_D_FCLTY_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_FCLTY from PeopleSoft table PS_D_FCLTY.
--
 --V01  SMT-xxxx 03/22/2018,    Srikanth,pabbu
--                              Converted from DataStage
--V02 2/12/2021            -- Srikanth,Pabbu made changes to FCLTY_SID field
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_FCLTY';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;

BEGIN
strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_FCLTY';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_FCLTY';

merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_FCLTY T
using (
with X as (  
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       Q1 as (  
select SETID, FACILITY_ID FCLTY_ID, SRC_SYS_ID, EFFDT, EFF_STATUS EFF_STAT_CD,  
       DESCRSHORT FCLTY_SD, DESCR FCLTY_LD,  
       LOCATION LOC_ID, BLDG_CD, FACILITY_TYPE FCLTY_TYPE_CD, ROOM ROOM_NM, ROOM_CAPACITY ROOM_CAPACITY_NUM, 
       DATA_ORIGIN,  
       row_number() over (partition by SETID, FACILITY_ID, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_FACILITY_TBL),
       Q2 as (  
select BLDG_CD, SRC_SYS_ID, EFFDT, 
       DESCRSHORT BLDG_SD, DESCR BLDG_LD,  
       DATA_ORIGIN,  
       row_number() over (partition by BLDG_CD, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_BLDG_TBL
 where DATA_ORIGIN <> 'D'),
       S as (
select Q1.SETID, Q1.FCLTY_ID, Q1.SRC_SYS_ID, Q1.EFFDT, Q1.EFF_STAT_CD,  
       Q1.FCLTY_SD, Q1.FCLTY_LD, 
       nvl(L1.LOC_SID,2147483646) LOC_SID, 
       Q1.BLDG_CD, nvl(Q2.BLDG_SD,'-') BLDG_SD, nvl(Q2.BLDG_LD,'-') BLDG_LD,  
       Q1.FCLTY_TYPE_CD, nvl(X1.XLATSHORTNAME,'-') FCLTY_TYPE_SD, nvl(X1.XLATLONGNAME,'-') FCLTY_TYPE_LD, 
       Q1.ROOM_NM, Q1.ROOM_CAPACITY_NUM,  
       Q1.DATA_ORIGIN  
  from Q1
  left outer join X X1
    on Q1.FCLTY_TYPE_CD = X1.FIELDVALUE
   and Q1.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'FACILITY_TYPE'
   and X1.X_ORDER = 1
  left outer join Q2
    on Q1.BLDG_CD = Q2.BLDG_CD
   and Q1.SRC_SYS_ID = Q2.SRC_SYS_ID
   and Q2.Q_ORDER = 1
  left outer join PS_D_LOCATION L1  
    on Q1.SETID = L1.SETID
   and Q1.LOC_ID = L1.LOC_ID 
   and Q1.SRC_SYS_ID = L1.SRC_SYS_ID
   and L1.DATA_ORIGIN <> 'D'
 where Q1.Q_ORDER = 1)
select nvl(D.FCLTY_SID, --max(D.FCLTY_SID) over (partition by 1) + This code does not ignore SID 2147483646 and below line will fix the issue and is added on 2/12/2021
(select nvl(max(FCLTY_SID),0) from CSMRT_OWNER.PS_D_FCLTY where FCLTY_SID <> 2147483646) +
       row_number() over (partition by 1 order by D.FCLTY_SID nulls first)) FCLTY_SID,
       nvl(D.SETID, S.SETID) SETID,
       nvl(D.FCLTY_ID, S.FCLTY_ID) FCLTY_ID,
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,
       decode(D.EFFDT, S.EFFDT, D.EFFDT, S.EFFDT) EFFDT,
       decode(D.EFF_STAT_CD, S.EFF_STAT_CD, D.EFF_STAT_CD, S.EFF_STAT_CD) EFF_STAT_CD,
       decode(D.FCLTY_SD, S.FCLTY_SD, D.FCLTY_SD, S.FCLTY_SD) FCLTY_SD,
       decode(D.FCLTY_LD, S.FCLTY_LD, D.FCLTY_LD, S.FCLTY_LD) FCLTY_LD,
       decode(D.LOC_SID, S.LOC_SID, D.LOC_SID, S.LOC_SID) LOC_SID,
       decode(D.BLDG_CD, S.BLDG_CD, D.BLDG_CD, S.BLDG_CD) BLDG_CD,
       decode(D.BLDG_SD, S.BLDG_SD, D.BLDG_SD, S.BLDG_SD) BLDG_SD,
       decode(D.BLDG_LD, S.BLDG_LD, D.BLDG_LD, S.BLDG_LD) BLDG_LD,
       decode(D.FCLTY_TYPE_CD, S.FCLTY_TYPE_CD, D.FCLTY_TYPE_CD, S.FCLTY_TYPE_CD) FCLTY_TYPE_CD,
       decode(D.FCLTY_TYPE_SD, S.FCLTY_TYPE_SD, D.FCLTY_TYPE_SD, S.FCLTY_TYPE_SD) FCLTY_TYPE_SD,
       decode(D.FCLTY_TYPE_LD, S.FCLTY_TYPE_LD, D.FCLTY_TYPE_LD, S.FCLTY_TYPE_LD) FCLTY_TYPE_LD,
       decode(D.ROOM_NM, S.ROOM_NM, D.ROOM_NM, S.ROOM_NM) ROOM_NM,
       decode(D.ROOM_CAPACITY_NUM, S.ROOM_CAPACITY_NUM, D.ROOM_CAPACITY_NUM, S.ROOM_CAPACITY_NUM) ROOM_CAPACITY_NUM,
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM
  from S
  left outer join CSMRT_OWNER.PS_D_FCLTY D
    on D.FCLTY_SID <> 2147483646
   and D.SETID = S.SETID
   and D.FCLTY_ID = S.FCLTY_ID
   and D.SRC_SYS_ID = S.SRC_SYS_ID
) S
    on  (T.SETID = S.SETID
   and  T.FCLTY_ID = S.FCLTY_ID
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)
 when matched then update set
       T.EFFDT = S.EFFDT,
       T.EFF_STAT_CD = S.EFF_STAT_CD,
       T.FCLTY_SD = S.FCLTY_SD,
       T.FCLTY_LD = S.FCLTY_LD,
       T.LOC_SID = S.LOC_SID,
       T.BLDG_CD = S.BLDG_CD,
       T.BLDG_SD = S.BLDG_SD,
       T.BLDG_LD = S.BLDG_LD,
       T.FCLTY_TYPE_CD = S.FCLTY_TYPE_CD,
       T.FCLTY_TYPE_SD = S.FCLTY_TYPE_SD,
       T.FCLTY_TYPE_LD = S.FCLTY_TYPE_LD,
       T.ROOM_NM = S.ROOM_NM,
       T.ROOM_CAPACITY_NUM = S.ROOM_CAPACITY_NUM,
       T.DATA_ORIGIN = S.DATA_ORIGIN,
       T.LASTUPD_EW_DTTM = SYSDATE
 where
       decode(T.EFFDT,S.EFFDT,0,1) = 1 or
       decode(T.EFF_STAT_CD,S.EFF_STAT_CD,0,1) = 1 or
       decode(T.FCLTY_SD,S.FCLTY_SD,0,1) = 1 or
       decode(T.FCLTY_LD,S.FCLTY_LD,0,1) = 1 or
       decode(T.LOC_SID,S.LOC_SID,0,1) = 1 or
       decode(T.BLDG_CD,S.BLDG_CD,0,1) = 1 or
       decode(T.BLDG_SD,S.BLDG_SD,0,1) = 1 or
       decode(T.BLDG_LD,S.BLDG_LD,0,1) = 1 or
       decode(T.FCLTY_TYPE_CD,S.FCLTY_TYPE_CD,0,1) = 1 or
       decode(T.FCLTY_TYPE_SD,S.FCLTY_TYPE_SD,0,1) = 1 or
       decode(T.FCLTY_TYPE_LD,S.FCLTY_TYPE_LD,0,1) = 1 or
       decode(T.ROOM_NM,S.ROOM_NM,0,1) = 1 or
       decode(T.ROOM_CAPACITY_NUM,S.ROOM_CAPACITY_NUM,0,1) = 1 or
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1
  when not matched then
insert (
       T.FCLTY_SID,
       T.SETID,
       T.FCLTY_ID,
       T.SRC_SYS_ID,
       T.EFFDT,
       T.EFF_STAT_CD,
       T.FCLTY_SD,
       T.FCLTY_LD,
       T.LOC_SID,
       T.BLDG_CD,
       T.BLDG_SD,
       T.BLDG_LD,
       T.FCLTY_TYPE_CD,
       T.FCLTY_TYPE_SD,
       T.FCLTY_TYPE_LD,
       T.ROOM_NM,
       T.ROOM_CAPACITY_NUM,
       T.DATA_ORIGIN,
       T.CREATED_EW_DTTM,
       T.LASTUPD_EW_DTTM)
values (
       S.FCLTY_SID,
       S.SETID,
       S.FCLTY_ID,
       S.SRC_SYS_ID,
       S.EFFDT,
       S.EFF_STAT_CD,
       S.FCLTY_SD,
       S.FCLTY_LD,
       S.LOC_SID,
       S.BLDG_CD,
       S.BLDG_SD,
       S.BLDG_LD,
       S.FCLTY_TYPE_CD,
       S.FCLTY_TYPE_SD,
       S.FCLTY_TYPE_LD,
       S.ROOM_NM,
       S.ROOM_CAPACITY_NUM,
       S.DATA_ORIGIN,
       SYSDATE,
       SYSDATE)
	   ;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_FCLTY rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_FCLTY',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_FCLTY';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.PS_D_FCLTY';
update CSMRT_OWNER.PS_D_FCLTY T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = sysdate
 where DATA_ORIGIN <> 'D'
   and T.FCLTY_SID < 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PS_FACILITY_TBL S
                    where T.SETID = S.SETID
                      and T.FCLTY_ID = S.FACILITY_ID
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_FCLTY rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_FCLTY',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
    WHEN OTHERS THEN

        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END PS_D_FCLTY_P;
/
