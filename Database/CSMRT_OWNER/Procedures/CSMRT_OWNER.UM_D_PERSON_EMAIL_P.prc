DROP PROCEDURE CSMRT_OWNER.UM_D_PERSON_EMAIL_P
/

--
-- UM_D_PERSON_EMAIL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_D_PERSON_EMAIL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
-- Old tables           -- UM_D_PERSON_EMAIL_AGG / UM_D_PERSON_CS_EMAIL_VW
-- Loads target table   -- UM_D_PERSON_EMAIL
-- UM_D_PERSON_EMAIL -- Dependent on PS_D_PERSON 
-- V01 4/2/2018         -- srikanth ,pabbu converted to proc from sql
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_PERSON_EMAIL';
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

strMessage01    := 'Merging data into CSSTG_OWNER.UM_D_PERSON_EMAIL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.UM_D_PERSON_EMAIL';

merge /*+ use_hash(S,T) */ into CSMRT_OWNER.UM_D_PERSON_EMAIL T 
using ( 
 with X as (  
select /*+ inline parallel(16) */ 
       FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       Q1 as (  
select /*+ inline parallel(16) */ 
       EMPLID PERSON_ID, E_ADDR_TYPE, SRC_SYS_ID, 
       EMAIL_ADDR, PREF_EMAIL_FLAG,
       DATA_ORIGIN
  from CSSTG_OWNER.PS_EMAIL_ADDRESSES), 
       Q2 as (  
select /*+ inline parallel(16) */ 
       Q1.PERSON_ID, Q1.E_ADDR_TYPE, Q1.SRC_SYS_ID, 
       Q1.EMAIL_ADDR, Q1.PREF_EMAIL_FLAG,
       nvl(X1.XLATSHORTNAME,'-') E_ADDR_TYPE_SD, nvl(X1.XLATLONGNAME,'-') E_ADDR_TYPE_LD,
       Q1.DATA_ORIGIN
  from Q1
  left outer join X X1
    on Q1.E_ADDR_TYPE = X1.FIELDVALUE
   and Q1.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'E_ADDR_TYPE'
   and X1.X_ORDER = 1),
       Q3 as (  
select /*+ inline parallel(16) */ 
       'UMBOS' INSTITUTION_CD,   
       Q2.PERSON_ID, Q2.E_ADDR_TYPE, Q2.SRC_SYS_ID, 
       Q2.EMAIL_ADDR, Q2.PREF_EMAIL_FLAG,
       Q2.E_ADDR_TYPE_SD, Q2.E_ADDR_TYPE_LD,
       Q2.DATA_ORIGIN
  from Q2
 where Q2.E_ADDR_TYPE not like 'UD%'
   and Q2.E_ADDR_TYPE not like 'UL%'
 union all
select /*+ inline parallel(16) */ 
       'UMDAR' INSTITUTION_CD,   
       Q2.PERSON_ID, Q2.E_ADDR_TYPE, Q2.SRC_SYS_ID, 
       Q2.EMAIL_ADDR, Q2.PREF_EMAIL_FLAG,
       Q2.E_ADDR_TYPE_SD, Q2.E_ADDR_TYPE_LD,
       Q2.DATA_ORIGIN
  from Q2
 where Q2.E_ADDR_TYPE not like 'UB%'
   and Q2.E_ADDR_TYPE not like 'UL%'
 union all
select /*+ inline parallel(16) */ 
       'UMLOW' INSTITUTION_CD,   
       Q2.PERSON_ID, Q2.E_ADDR_TYPE, Q2.SRC_SYS_ID, 
       Q2.EMAIL_ADDR, Q2.PREF_EMAIL_FLAG,
       Q2.E_ADDR_TYPE_SD, Q2.E_ADDR_TYPE_LD,
       Q2.DATA_ORIGIN
  from Q2
 where Q2.E_ADDR_TYPE not like 'UB%'
   and Q2.E_ADDR_TYPE not like 'UD%'), 
       S as (
select /*+ inline parallel(16) */ 
       nvl(Q3.INSTITUTION_CD,'-') INSTITUTION_CD, P.PERSON_ID, nvl(Q3.E_ADDR_TYPE,'-') E_ADDR_TYPE, P.SRC_SYS_ID, 
       nvl(Q3.EMAIL_ADDR,'-') EMAIL_ADDR, 
       P.PERSON_SID,   
       nvl(Q3.E_ADDR_TYPE_SD,'-') E_ADDR_TYPE_SD, nvl(Q3.E_ADDR_TYPE_LD,'-') E_ADDR_TYPE_LD, 
       nvl(Q3.PREF_EMAIL_FLAG,'-') PREF_EMAIL_FLAG, 
row_number() over (partition by P.PERSON_SID, nvl(Q3.INSTITUTION_CD,'-') 
                       order by (case when nvl(Q3.DATA_ORIGIN,'S') <> 'S' then 9
                                      when nvl(Q3.E_ADDR_TYPE,'-') = 'PERS' then 0
                                      when substr(nvl(Q3.E_ADDR_TYPE,'-'),3,2) = 'ST' then 1
                                      when substr(nvl(Q3.E_ADDR_TYPE,'-'),1,2) in ('UB','UD','UL') then 2
                                      else 9 end)) PS_EMAIL_ORDER, 
row_number() over (partition by P.PERSON_SID, nvl(Q3.INSTITUTION_CD,'-') 
                       order by (case when nvl(Q3.DATA_ORIGIN,'S') <> 'S' then 9
                                      when substr(nvl(Q3.E_ADDR_TYPE,'-'),3,2) = 'ST' then 0
                                      when nvl(Q3.E_ADDR_TYPE,'-') = 'PERS' then 1
                                      when substr(nvl(Q3.E_ADDR_TYPE,'-'),1,2) in ('UB','UD','UL') then 2
                                      else 9 end)) SP_EMAIL_ORDER, 
       least(P.DATA_ORIGIN,nvl(Q3.DATA_ORIGIN,'Z')) DATA_ORIGIN 
  from PS_D_PERSON P  
  left outer join Q3
    on P.PERSON_ID = Q3.PERSON_ID
   and P.SRC_SYS_ID = Q3.SRC_SYS_ID)
select /*+ parallel(16) */
       nvl(D.INSTITUTION_CD,S.INSTITUTION_CD) INSTITUTION_CD, 
       nvl(D.PERSON_ID, S.PERSON_ID) PERSON_ID, 
       nvl(D.E_ADDR_TYPE, S.E_ADDR_TYPE) E_ADDR_TYPE, 
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID, 
       decode(D.EMAIL_ADDR, S.EMAIL_ADDR, D.EMAIL_ADDR, S.EMAIL_ADDR) EMAIL_ADDR, 
       decode(D.PERSON_SID, S.PERSON_SID, D.PERSON_SID, S.PERSON_SID) PERSON_SID, 
       decode(D.E_ADDR_TYPE_SD, S.E_ADDR_TYPE_SD, D.E_ADDR_TYPE_SD, S.E_ADDR_TYPE_SD) E_ADDR_TYPE_SD, 
       decode(D.E_ADDR_TYPE_LD, S.E_ADDR_TYPE_LD, D.E_ADDR_TYPE_LD, S.E_ADDR_TYPE_LD) E_ADDR_TYPE_LD, 
       decode(D.PREF_EMAIL_FLAG, S.PREF_EMAIL_FLAG, D.PREF_EMAIL_FLAG, S.PREF_EMAIL_FLAG) PREF_EMAIL_FLAG, 
       decode(D.PS_EMAIL_ORDER, S.PS_EMAIL_ORDER, D.PS_EMAIL_ORDER, S.PS_EMAIL_ORDER) PS_EMAIL_ORDER, 
       decode(D.SP_EMAIL_ORDER, S.SP_EMAIL_ORDER, D.SP_EMAIL_ORDER, S.SP_EMAIL_ORDER) SP_EMAIL_ORDER, 
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN, 
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM, 
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM 
  from S 
  left outer join CSMRT_OWNER.UM_D_PERSON_EMAIL D 
    on D.INSTITUTION_CD= S.INSTITUTION_CD 
   and D.PERSON_ID = S.PERSON_ID 
   and D.E_ADDR_TYPE = S.E_ADDR_TYPE 
   and D.SRC_SYS_ID = S.SRC_SYS_ID 
) S                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
    on  (T.PERSON_ID = S.PERSON_ID 
   and  T.E_ADDR_TYPE = S.E_ADDR_TYPE 
   and  T.SRC_SYS_ID = S.SRC_SYS_ID 
   and  S.INSTITUTION_CD=T.INSTITUTION_CD) 
 when matched then update set 
       T.EMAIL_ADDR = S.EMAIL_ADDR, 
       T.PERSON_SID = S.PERSON_SID, 
       T.E_ADDR_TYPE_SD = S.E_ADDR_TYPE_SD, 
       T.E_ADDR_TYPE_LD = S.E_ADDR_TYPE_LD, 
       T.PREF_EMAIL_FLAG = S.PREF_EMAIL_FLAG, 
       T.PS_EMAIL_ORDER = S.PS_EMAIL_ORDER, 
       T.SP_EMAIL_ORDER = S.SP_EMAIL_ORDER, 
       T.DATA_ORIGIN = S.DATA_ORIGIN, 
       T.LASTUPD_EW_DTTM = SYSDATE 
 where 
       decode(T.EMAIL_ADDR,S.EMAIL_ADDR,0,1) = 1 or 
       decode(T.PERSON_SID,S.PERSON_SID,0,1) = 1 or 
       decode(T.E_ADDR_TYPE_SD,S.E_ADDR_TYPE_SD,0,1) = 1 or 
       decode(T.E_ADDR_TYPE_LD,S.E_ADDR_TYPE_LD,0,1) = 1 or 
       decode(T.PREF_EMAIL_FLAG,S.PREF_EMAIL_FLAG,0,1) = 1 or 
       decode(T.PS_EMAIL_ORDER,S.PS_EMAIL_ORDER,0,1) = 1 or 
       decode(T.SP_EMAIL_ORDER,S.SP_EMAIL_ORDER,0,1) = 1 or 
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1 
  when not matched then 
insert ( 
       T.INSTITUTION_CD,
       T.PERSON_ID,     
       T.E_ADDR_TYPE,   
       T.SRC_SYS_ID,    
       T.EMAIL_ADDR,    
       T.PERSON_SID,    
       T.E_ADDR_TYPE_SD,
       T.E_ADDR_TYPE_LD,
       T.PREF_EMAIL_FLAG,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       T.PS_EMAIL_ORDER,
       T.SP_EMAIL_ORDER,
       T.DATA_ORIGIN,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       T.CREATED_EW_DTTM,
       T.LASTUPD_EW_DTTM)
values ( 
       S.INSTITUTION_CD,
       S.PERSON_ID, 
       S.E_ADDR_TYPE,
       S.SRC_SYS_ID,
       S.EMAIL_ADDR,
       S.PERSON_SID,
       S.E_ADDR_TYPE_SD,
       S.E_ADDR_TYPE_LD,
       S.PREF_EMAIL_FLAG,
       S.PS_EMAIL_ORDER,
       S.SP_EMAIL_ORDER,
       S.DATA_ORIGIN,
       SYSDATE,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       SYSDATE); 

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_PERSON_EMAIL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_PERSON_EMAIL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.UM_D_PERSON_EMAIL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.UM_D_PERSON_EMAIL';
update CSMRT_OWNER.UM_D_PERSON_EMAIL T 
   set DATA_ORIGIN = 'D', 
       LASTUPD_EW_DTTM = SYSDATE 
 where T.DATA_ORIGIN <> 'D'
   and T.E_ADDR_TYPE <> '-' 
   and not exists (select 1 
                     from CSSTG_OWNER.PS_EMAIL_ADDRESSES S 
                    where T.PERSON_ID = S.EMPLID 
                      and T.E_ADDR_TYPE = S.E_ADDR_TYPE 
                      and T.SRC_SYS_ID = S.SRC_SYS_ID 
                      and S.DATA_ORIGIN <> 'D');

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_PERSON_EMAIL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_PERSON_EMAIL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.UM_D_PERSON_EMAIL';
update CSMRT_OWNER.UM_D_PERSON_EMAIL T 
   set DATA_ORIGIN = 'D', 
       LASTUPD_EW_DTTM = SYSDATE 
 where T.DATA_ORIGIN <> 'D' 
   and not exists (select 1
                     from CSMRT_OWNER.PS_D_PERSON P 
                    where T.PERSON_ID = P.PERSON_ID 
                      and T.SRC_SYS_ID = P.SRC_SYS_ID 
                      and P.DATA_ORIGIN <> 'D'); 

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_PERSON_EMAIL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_PERSON_EMAIL',
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

END UM_D_PERSON_EMAIL_P;
/
