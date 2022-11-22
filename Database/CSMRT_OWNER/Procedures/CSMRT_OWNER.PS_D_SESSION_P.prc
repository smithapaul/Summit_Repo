DROP PROCEDURE CSMRT_OWNER.PS_D_SESSION_P
/

--
-- PS_D_SESSION_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_D_SESSION_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--OLD tables               -- PS_D_SESSION
--Loads target table       -- PS_D_SESSION
--PS_D_SESSION             --Dependent on PS_D_INSTITUTION -100, PS_D_ACAD_CAR -100, PS_D_TERM -200
-- V01 4/16/2018             -- srikanth ,pabbu converted to proc from sql
-- V02 2/12/2021             --Srikanth,Pabbu made changes to SESSION_SID field
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_SESSION';
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

strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_SESSION';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_SESSION';

merge /* parallel(T,8) */ /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_SESSION T   
using (
with TP as (
select INSTITUTION, ACAD_CAREER, STRM, SESSION_CODE, SRC_SYS_ID, 
       min(case when TIME_PERIOD in ('010') then END_DT else NULL end) REGISTR_START_DT,
       max(case when TIME_PERIOD in ('025','030','040') then END_DT else NULL end) ADD_DROP_END_DT,
       max(case when TIME_PERIOD in ('072') then END_DT else NULL end) WITHDRAW_END_DT
  from CSSTG_OWNER.PS_SESS_TIME_PEROD
 where DATA_ORIGIN <> 'D'
 group by INSTITUTION, ACAD_CAREER, STRM, SESSION_CODE, SRC_SYS_ID),
       X as (  
select /*+ inline */ 
       FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
S as (
select S.INSTITUTION INSTITUTION_CD, 
       S.ACAD_CAREER ACAD_CAR_CD, 
       S.STRM TERM_CD, 
       S.SESSION_CODE SESSION_CD, 
       S.SRC_SYS_ID,
       nvl(X1.XLATSHORTNAME,'-') SESSION_SD, 
       nvl(X1.XLATLONGNAME,'-') SESSION_LD,
       nvl(I.INSTITUTION_SID,2147483646) INSTITUTION_SID, 
       nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID, 
       nvl(T.TERM_SID,2147483646) TERM_SID, 
       S.SESS_BEGIN_DT SESSION_BEGIN_DT, 
       S.SESS_END_DT SESSION_END_DT, 
       S.ENROLL_OPEN_DT OPEN_ENRLMT_DT, 
       S.FIRST_ENRL_DT FIRST_ENRLMT_DT, 
       S.LAST_ENRL_DT LAST_ENRLMT_DT, 
       S.LAST_WAIT_DT LAST_WAIT_LIST_DT, 
       S.SIXTY_PCT_DT, 
       S.CENSUS_DT, 
       TP.REGISTR_START_DT,
       TP.ADD_DROP_END_DT,
       TP.WITHDRAW_END_DT,
       S.WEEKS_OF_INSTRUCT INSTRCTN_WEEK_NUM, 
       S.DATA_ORIGIN, 
       SYSDATE CREATED_EW_DTTM, 
       SYSDATE LASTUPD_EW_DTTM 
  from CSSTG_OWNER.PS_SESSION_TBL S 
  left outer join PS_D_INSTITUTION I       
    on S.INSTITUTION = I.INSTITUTION_CD
   and S.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join PS_D_ACAD_CAR C          
    on S.INSTITUTION = C.INSTITUTION_CD
   and S.ACAD_CAREER = C.ACAD_CAR_CD
   and S.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join PS_D_TERM T              
    on S.INSTITUTION = T.INSTITUTION_CD
   and S.ACAD_CAREER = T.ACAD_CAR_CD
   and S.STRM = T.TERM_CD
   and S.SRC_SYS_ID = T.SRC_SYS_ID
   and T.DATA_ORIGIN <> 'D'
  left outer join TP
    on S.INSTITUTION = TP.INSTITUTION 
   and S.ACAD_CAREER = TP.ACAD_CAREER
   and S.STRM = TP.STRM
   and S.SESSION_CODE = TP.SESSION_CODE   
   and S.SRC_SYS_ID = TP.SRC_SYS_ID
  left outer join X X1
    on S.SESSION_CODE = X1.FIELDVALUE
   and S.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'SESSION_CODE'
   and X1.X_ORDER = 1  
)
select nvl(D.SESSION_SID, --max(D.SESSION_SID) over (partition by 1) + This code does not ignore SID 2147483646 and below line will fix the issue and is added on 2/12/2021
(select nvl(max(SESSION_SID),0) from CSMRT_OWNER.PS_D_SESSION where SESSION_SID <> 2147483646) + 
       row_number() over (partition by 1 order by D.SESSION_SID nulls first)) SESSION_SID, 
       nvl(D.INSTITUTION_CD, S.INSTITUTION_CD) INSTITUTION_CD, 
       nvl(D.ACAD_CAR_CD, S.ACAD_CAR_CD) ACAD_CAR_CD, 
       nvl(D.TERM_CD, S.TERM_CD) TERM_CD, 
       nvl(D.SESSION_CD, S.SESSION_CD) SESSION_CD, 
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID, 
       decode(D.SESSION_SD,S.SESSION_SD,D.SESSION_SD,S.SESSION_SD) SESSION_SD,
       decode(D.SESSION_LD,S.SESSION_LD,D.SESSION_LD,S.SESSION_LD) SESSION_LD,
       decode(D.INSTITUTION_SID,S.INSTITUTION_SID,D.INSTITUTION_SID,S.INSTITUTION_SID) INSTITUTION_SID,
       decode(D.ACAD_CAR_SID,S.ACAD_CAR_SID,D.ACAD_CAR_SID,S.ACAD_CAR_SID) ACAD_CAR_SID,
       decode(D.TERM_SID,S.TERM_SID,D.TERM_SID,S.TERM_SID) TERM_SID,
       decode(D.SESSION_BEGIN_DT,S.SESSION_BEGIN_DT,D.SESSION_BEGIN_DT,S.SESSION_BEGIN_DT) SESSION_BEGIN_DT,
       decode(D.SESSION_END_DT,S.SESSION_END_DT,D.SESSION_END_DT,S.SESSION_END_DT) SESSION_END_DT,
       decode(D.OPEN_ENRLMT_DT,S.OPEN_ENRLMT_DT,D.OPEN_ENRLMT_DT,S.OPEN_ENRLMT_DT) OPEN_ENRLMT_DT,
       decode(D.FIRST_ENRLMT_DT,S.FIRST_ENRLMT_DT,D.FIRST_ENRLMT_DT,S.FIRST_ENRLMT_DT) FIRST_ENRLMT_DT,
       decode(D.LAST_ENRLMT_DT,S.LAST_ENRLMT_DT,D.LAST_ENRLMT_DT,S.LAST_ENRLMT_DT) LAST_ENRLMT_DT,
       decode(D.LAST_WAIT_LIST_DT,S.LAST_WAIT_LIST_DT,D.LAST_WAIT_LIST_DT,S.LAST_WAIT_LIST_DT) LAST_WAIT_LIST_DT,
       decode(D.SIXTY_PCT_DT,S.SIXTY_PCT_DT,D.SIXTY_PCT_DT,S.SIXTY_PCT_DT) SIXTY_PCT_DT,
       decode(D.CENSUS_DT,S.CENSUS_DT,D.CENSUS_DT,S.CENSUS_DT) CENSUS_DT,
       decode(D.REGISTR_START_DT,S.REGISTR_START_DT,D.REGISTR_START_DT,S.REGISTR_START_DT) REGISTR_START_DT,
       decode(D.ADD_DROP_END_DT,S.ADD_DROP_END_DT,D.ADD_DROP_END_DT,S.ADD_DROP_END_DT) ADD_DROP_END_DT,
       decode(D.WITHDRAW_END_DT,S.WITHDRAW_END_DT,D.WITHDRAW_END_DT,S.WITHDRAW_END_DT) WITHDRAW_END_DT,
       decode(D.INSTRCTN_WEEK_NUM,S.INSTRCTN_WEEK_NUM,D.INSTRCTN_WEEK_NUM,S.INSTRCTN_WEEK_NUM) INSTRCTN_WEEK_NUM,
       decode(D.DATA_ORIGIN,S.DATA_ORIGIN,D.DATA_ORIGIN,S.DATA_ORIGIN) DATA_ORIGIN,
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM, 
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM 
  from S
  left outer join CSMRT_OWNER.PS_D_SESSION D    
    on D.INSTITUTION_CD = S.INSTITUTION_CD   
   and D.ACAD_CAR_CD = S.ACAD_CAR_CD
   and D.TERM_CD = S.TERM_CD
   and D.SESSION_CD = S.SESSION_CD  
   and D.SRC_SYS_ID = S.SRC_SYS_ID 
   and D.SESSION_SID < 2147483646) S 
   on (T.INSTITUTION_CD = S.INSTITUTION_CD
  and  T.ACAD_CAR_CD = S.ACAD_CAR_CD
  and  T.TERM_CD = S.TERM_CD
  and  T.SESSION_CD = S.SESSION_CD
  and  T.SRC_SYS_ID = S.SRC_SYS_ID) 
 when matched then update set 
       T.SESSION_SD = S.SESSION_SD,
       T.SESSION_LD = S.SESSION_LD,
       T.INSTITUTION_SID = S.INSTITUTION_SID,
       T.ACAD_CAR_SID = S.ACAD_CAR_SID,
       T.TERM_SID = S.TERM_SID,
       T.SESSION_BEGIN_DT = S.SESSION_BEGIN_DT,
       T.SESSION_END_DT = S.SESSION_END_DT,
       T.OPEN_ENRLMT_DT = S.OPEN_ENRLMT_DT,
       T.FIRST_ENRLMT_DT = S.FIRST_ENRLMT_DT,
       T.LAST_ENRLMT_DT = S.LAST_ENRLMT_DT,
       T.LAST_WAIT_LIST_DT = S.LAST_WAIT_LIST_DT,
       T.SIXTY_PCT_DT = S.SIXTY_PCT_DT,
       T.CENSUS_DT = S.CENSUS_DT,
       T.REGISTR_START_DT = S.REGISTR_START_DT,
       T.ADD_DROP_END_DT = S.ADD_DROP_END_DT,
       T.WITHDRAW_END_DT = S.WITHDRAW_END_DT,
       T.INSTRCTN_WEEK_NUM = S.INSTRCTN_WEEK_NUM,
       T.DATA_ORIGIN = S.DATA_ORIGIN,
       T.LASTUPD_EW_DTTM = SYSDATE
where 
       decode(T.SESSION_SD,S.SESSION_SD,0,1) = 1 or
       decode(T.SESSION_LD,S.SESSION_LD,0,1) = 1 or
       decode(T.INSTITUTION_SID,S.INSTITUTION_SID,0,1) = 1 or
       decode(T.ACAD_CAR_SID,S.ACAD_CAR_SID,0,1) = 1 or
       decode(T.TERM_SID,S.TERM_SID,0,1) = 1 or
       decode(T.SESSION_BEGIN_DT,S.SESSION_BEGIN_DT,0,1) = 1 or
       decode(T.SESSION_END_DT,S.SESSION_END_DT,0,1) = 1 or
       decode(T.OPEN_ENRLMT_DT,S.OPEN_ENRLMT_DT,0,1) = 1 or
       decode(T.FIRST_ENRLMT_DT,S.FIRST_ENRLMT_DT,0,1) = 1 or
       decode(T.LAST_ENRLMT_DT,S.LAST_ENRLMT_DT,0,1) = 1 or
       decode(T.LAST_WAIT_LIST_DT,S.LAST_WAIT_LIST_DT,0,1) = 1 or
       decode(T.SIXTY_PCT_DT,S.SIXTY_PCT_DT,0,1) = 1 or
       decode(T.CENSUS_DT,S.CENSUS_DT,0,1) = 1 or
       decode(T.REGISTR_START_DT,S.REGISTR_START_DT,0,1) = 1 or
       decode(T.ADD_DROP_END_DT,S.ADD_DROP_END_DT,0,1) = 1 or
       decode(T.WITHDRAW_END_DT,S.WITHDRAW_END_DT,0,1) = 1 or
       decode(T.INSTRCTN_WEEK_NUM,S.INSTRCTN_WEEK_NUM,0,1) = 1 or
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1  
 when not matched then
insert (
       T.SESSION_SID,
       T.INSTITUTION_CD,
       T.ACAD_CAR_CD,
       T.TERM_CD,
       T.SESSION_CD,
       T.SRC_SYS_ID,
       T.SESSION_SD,
       T.SESSION_LD,
       T.INSTITUTION_SID,
       T.ACAD_CAR_SID,
       T.TERM_SID,
       T.SESSION_BEGIN_DT,
       T.SESSION_END_DT,
       T.OPEN_ENRLMT_DT,
       T.FIRST_ENRLMT_DT,
       T.LAST_ENRLMT_DT,
       T.LAST_WAIT_LIST_DT,
       T.SIXTY_PCT_DT,
       T.CENSUS_DT,
       T.REGISTR_START_DT,
       T.ADD_DROP_END_DT,
       T.WITHDRAW_END_DT,
       T.INSTRCTN_WEEK_NUM,
       T.DATA_ORIGIN,
       T.CREATED_EW_DTTM,
       T.LASTUPD_EW_DTTM)
values (
       S.SESSION_SID,
       S.INSTITUTION_CD,
       S.ACAD_CAR_CD,
       S.TERM_CD,
       S.SESSION_CD,
       S.SRC_SYS_ID,
       S.SESSION_SD,
       S.SESSION_LD,
       S.INSTITUTION_SID,
       S.ACAD_CAR_SID,
       S.TERM_SID,
       S.SESSION_BEGIN_DT,
       S.SESSION_END_DT,
       S.OPEN_ENRLMT_DT,
       S.FIRST_ENRLMT_DT,
       S.LAST_ENRLMT_DT,
       S.LAST_WAIT_LIST_DT,
       S.SIXTY_PCT_DT,
       S.CENSUS_DT,
       S.REGISTR_START_DT,
       S.ADD_DROP_END_DT,
       S.WITHDRAW_END_DT,
       S.INSTRCTN_WEEK_NUM,
       S.DATA_ORIGIN,
       SYSDATE,
       SYSDATE)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_SESSION rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_SESSION',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_SESSION';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.PS_D_SESSION';

update CSMRT_OWNER.PS_D_SESSION T  
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.SESSION_SID < 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PS_SESSION_TBL S
                    where T.INSTITUTION_CD = S.INSTITUTION
                      and T.ACAD_CAR_CD = S.ACAD_CAREER
                      and T.TERM_CD = S.STRM
                      and T.SESSION_CD = S.SESSION_CODE
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
					  and S.DATA_ORIGIN <> 'D');


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_SESSION rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_SESSION',
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

END PS_D_SESSION_P;
/
