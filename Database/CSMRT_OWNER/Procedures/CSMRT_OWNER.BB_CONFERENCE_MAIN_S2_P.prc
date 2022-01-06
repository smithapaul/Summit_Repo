CREATE OR REPLACE PROCEDURE             "BB_CONFERENCE_MAIN_S2_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Jim Doucette
--
-- Loads S2 table BB_CONFERENCE_MAIN_S2 from S1 table BB_CONFERENCE_MAIN_S1.
--
-- V01  CASE-xxxxx 08/28/2020,    Jim Doucette
--                                New
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'BB_CONFERENCE_MAIN_S2';
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

strMessage01    := 'Merging data into CSSTG_OWNER.BB_CONFERENCE_MAIN_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.BB_CONFERENCE_MAIN_S2';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.BB_CONFERENCE_MAIN_S2 T
using (select /*+ full(S) */
              BB_SOURCE, 
              PK1, 
              DTCREATED, 
              DTMODIFIED, 
              CRSMAIN_PK1, 
              GROUPS_PK1, 
              AVAILABLE_IND, 
              TEXT_FORMAT_TYPE, 
              ORDER_NUM, 
              NAME, 
              ICON
  from CSSTG_OWNER.BB_CONFERENCE_MAIN_S1) S
 on ( 
    T.BB_SOURCE = S.BB_SOURCE and 
    T.PK1 = S.PK1)
 when matched then update set
    T.DTCREATED = S.DTCREATED,
    T.DTMODIFIED = S.DTMODIFIED,
    T.CRSMAIN_PK1 = S.CRSMAIN_PK1,
    T.GROUPS_PK1 = S.GROUPS_PK1,
    T.AVAILABLE_IND = S.AVAILABLE_IND,
    T.TEXT_FORMAT_TYPE = S.TEXT_FORMAT_TYPE,
    T.ORDER_NUM = S.ORDER_NUM,
	T.NAME = S.NAME,
	T.ICON = S.ICON,
	T.DELETE_FLAG = 'N',
	T.UPDATE_TIME = SYSDATE
where 
    nvl(trim(T.DTCREATED),0) <> nvl(trim(S.DTCREATED),0) or 
    nvl(trim(T.DTMODIFIED),0) <> nvl(trim(S.DTMODIFIED),0) or 
    nvl(trim(T.CRSMAIN_PK1),0) <> nvl(trim(S.CRSMAIN_PK1),0) or 
    nvl(trim(T.GROUPS_PK1),0) <> nvl(trim(S.GROUPS_PK1),0) or 
    nvl(trim(T.AVAILABLE_IND),0) <> nvl(trim(S.AVAILABLE_IND),0) or 
    nvl(trim(T.TEXT_FORMAT_TYPE),0) <> nvl(trim(S.TEXT_FORMAT_TYPE),0) or 
    nvl(trim(T.ORDER_NUM),0) <> nvl(trim(S.ORDER_NUM),0) or 
    nvl(trim(T.NAME),0) <> nvl(trim(S.NAME),0) or 
    nvl(trim(T.ICON),0) <> nvl(trim(S.ICON),0)
when not matched then 
insert (
    T.BB_SOURCE,
    T.PK1,     
    T.DTCREATED,            
    T.DTMODIFIED,                      
	T.CRSMAIN_PK1,          
	T.GROUPS_PK1,                    
	T.AVAILABLE_IND,        
	T.TEXT_FORMAT_TYPE,              
	T.ORDER_NUM,         
	T.NAME,      
	T.ICON,           
	T.DELETE_FLAG,          
	T.INSERT_TIME,          
	T.UPDATE_TIME    
) 
values (
    S.BB_SOURCE,
    S.PK1,     
    S.DTCREATED,            
    S.DTMODIFIED,                      
    S.CRSMAIN_PK1,          
    S.GROUPS_PK1,                    
    S.AVAILABLE_IND,        
    S.TEXT_FORMAT_TYPE,              
    S.ORDER_NUM,         
    S.NAME,      
    S.ICON,           
    'N',          
    SYSDATE,          
    SYSDATE)
	;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of BB_CONFERENCE_MAIN_S2 rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'BB_CONFERENCE_MAIN_S2',
                i_Action            => 'MERGE',
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

END BB_CONFERENCE_MAIN_S2_P;
/
