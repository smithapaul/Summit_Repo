CREATE OR REPLACE PROCEDURE             "UM_D_CLASS_CHRSTC_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table UM_D_CLASS_CHRSTC from PeopleSoft table UM_D_CLASS_CHRSTC.
--
 --V01  SMT-7588 02/09/2018,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_CLASS_CHRSTC';
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

strMessage01    := 'Merging data into CSMRT_OWNER.UM_D_CLASS_CHRSTC';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.UM_D_CLASS_CHRSTC';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.UM_D_CLASS_CHRSTC T 
using ( 
  with Q1 as (  
select ROOM_CHRSTC, 
       SRC_SYS_ID, 
	   EFFDT, 
	   EFF_STATUS, 
       DESCRSHORT, 
	   DESCR,  
       DATA_ORIGIN,
       row_number() over (partition by ROOM_CHRSTC, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_ROOM_CHRSTC_TBL
 where DATA_ORIGIN <> 'D'), 
       Q2 as ( 
select CRSE_ID CRSE_CD, CRSE_OFFER_NBR CRSE_OFFER_NUM, STRM TERM_CD, SESSION_CODE SESSION_CD, CLASS_SECTION CLASS_SECTION_CD, 
       ROOM_CHRSTC, SRC_SYS_ID,  
       DATA_ORIGIN
  from CSSTG_OWNER.PS_CLASS_CHRSTC), 
       S as (
select C.CRSE_CD, C.CRSE_OFFER_NUM, C.TERM_CD, C.SESSION_CD, C.CLASS_SECTION_CD, 
       nvl(Q2.ROOM_CHRSTC,'-') ROOM_CHRSTC, C.SRC_SYS_ID, 
       C.INSTITUTION_CD, C.CLASS_SID, 
       nvl(Q1.DESCRSHORT,'-') DESCRSHORT, nvl(Q1.DESCR,'-') DESCR, 
       least(C.DATA_ORIGIN,nvl(Q2.DATA_ORIGIN,'Z')) DATA_ORIGIN  
  from CSMRT_OWNER.UM_D_CLASS C
  join Q2                           -- Inner join 
    on C.CRSE_CD = Q2.CRSE_CD
   and C.CRSE_OFFER_NUM = Q2.CRSE_OFFER_NUM
   and C.TERM_CD = Q2.TERM_CD
   and C.SESSION_CD = Q2.SESSION_CD
   and C.CLASS_SECTION_CD = Q2.CLASS_SECTION_CD
   and C.SRC_SYS_ID = Q2.SRC_SYS_ID
  left outer join Q1
    on Q2.ROOM_CHRSTC = Q1.ROOM_CHRSTC
   and Q2.SRC_SYS_ID = Q1.SRC_SYS_ID
   and Q1.Q_ORDER = 1
    ) 
select nvl(D.CRSE_CD, S.CRSE_CD) CRSE_CD, 
       nvl(D.CRSE_OFFER_NUM, S.CRSE_OFFER_NUM) CRSE_OFFER_NUM, 
       nvl(D.TERM_CD, S.TERM_CD) TERM_CD, 
       nvl(D.SESSION_CD, S.SESSION_CD) SESSION_CD, 
       nvl(D.CLASS_SECTION_CD, S.CLASS_SECTION_CD) CLASS_SECTION_CD, 
       nvl(D.ROOM_CHRSTC, S.ROOM_CHRSTC) ROOM_CHRSTC, 
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID, 
       decode(D.INSTITUTION_CD, S.INSTITUTION_CD, D.INSTITUTION_CD, S.INSTITUTION_CD) INSTITUTION_CD, 
       decode(D.CLASS_SID, S.CLASS_SID, D.CLASS_SID, S.CLASS_SID) CLASS_SID, 
       decode(D.DESCRSHORT, S.DESCRSHORT, D.DESCRSHORT, S.DESCRSHORT) DESCRSHORT, 
       decode(D.DESCR, S.DESCR, D.DESCR, S.DESCR) DESCR, 
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN, 
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM, 
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM 
  from S                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
  left outer join CSMRT_OWNER.UM_D_CLASS_CHRSTC D 
    on D.CRSE_CD = S.CRSE_CD 	
   and D.CRSE_OFFER_NUM = S.CRSE_OFFER_NUM                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
   and D.TERM_CD = S.TERM_CD                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
   and D.SESSION_CD = S.SESSION_CD                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
   and D.CLASS_SECTION_CD = S.CLASS_SECTION_CD                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
   and D.ROOM_CHRSTC = S.ROOM_CHRSTC                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
   and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
) S                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
    on (T.CRSE_CD = S.CRSE_CD 	
   and  T.CRSE_OFFER_NUM = S.CRSE_OFFER_NUM 
   and  T.TERM_CD = S.TERM_CD 
   and  T.SESSION_CD = S.SESSION_CD 
   and  T.CLASS_SECTION_CD = S.CLASS_SECTION_CD 
   and  T.ROOM_CHRSTC = S.ROOM_CHRSTC 
   and  T.SRC_SYS_ID = S.SRC_SYS_ID) 
 when matched then update set 
       T.INSTITUTION_CD = S.INSTITUTION_CD, 
       T.CLASS_SID = S.CLASS_SID, 
       T.DESCRSHORT = S.DESCRSHORT, 
       T.DESCR = S.DESCR, 
       T.DATA_ORIGIN = S.DATA_ORIGIN, 
       T.LASTUPD_EW_DTTM = SYSDATE 
 where 
       decode(T.INSTITUTION_CD,S.INSTITUTION_CD,0,1) = 1 or 
       decode(T.CLASS_SID,S.CLASS_SID,0,1) = 1 or 
       decode(T.DESCRSHORT,S.DESCRSHORT,0,1) = 1 or 
       decode(T.DESCR,S.DESCR,0,1) = 1 or 
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1 
  when not matched then 
insert ( 
       T.CRSE_CD, 
       T.CRSE_OFFER_NUM, 
       T.TERM_CD, 
       T.SESSION_CD, 
       T.CLASS_SECTION_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
       T.ROOM_CHRSTC, 
       T.SRC_SYS_ID, 
       T.INSTITUTION_CD, 
       T.CLASS_SID, 
       T.DESCRSHORT, 
       T.DESCR, 
       T.DATA_ORIGIN, 
       T.CREATED_EW_DTTM, 
       T.LASTUPD_EW_DTTM) 
values ( 
       S.CRSE_CD, 
       S.CRSE_OFFER_NUM, 
       S.TERM_CD, 
       S.SESSION_CD, 
       S.CLASS_SECTION_CD, 
       S.ROOM_CHRSTC, 
       S.SRC_SYS_ID, 
       S.INSTITUTION_CD, 
       S.CLASS_SID, 
       S.DESCRSHORT, 
       S.DESCR, 
       S.DATA_ORIGIN, 
       SYSDATE, 
       SYSDATE) 
; 

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_CLASS_CHRSTC rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_CLASS_CHRSTC',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.UM_D_CLASS_CHRSTC';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.UM_D_CLASS_CHRSTC';
update CSMRT_OWNER.UM_D_CLASS_CHRSTC T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and not exists (select 1
                     from CSMRT_OWNER.UM_D_CLASS S  
                    where T.CRSE_CD = S.CRSE_CD
                      and T.CRSE_OFFER_NUM = S.CRSE_OFFER_NUM
                      and T.TERM_CD = S.TERM_CD
                      and T.SESSION_CD = S.SESSION_CD
                      and T.CLASS_SECTION_CD = S.CLASS_SECTION_CD
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_CLASS_CHRSTC rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_CLASS_CHRSTC',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.UM_D_CLASS_CHRSTC';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.UM_D_CLASS_CHRSTC';
 update CSMRT_OWNER.UM_D_CLASS_CHRSTC T 	
    set DATA_ORIGIN = 'D',
        LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and not exists (select 1
                     from CSSTG_OWNER.PS_CLASS_CHRSTC S
                    where T.CRSE_CD = S.CRSE_ID
                      and T.CRSE_OFFER_NUM = S.CRSE_OFFER_NBR 
                      and T.TERM_CD = S.STRM
                      and T.SESSION_CD = S.SESSION_CODE 
                      and T.CLASS_SECTION_CD = S.CLASS_SECTION
                      and T.ROOM_CHRSTC = S.ROOM_CHRSTC
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_CLASS_CHRSTC rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_CLASS_CHRSTC',
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

END UM_D_CLASS_CHRSTC_P;
/
