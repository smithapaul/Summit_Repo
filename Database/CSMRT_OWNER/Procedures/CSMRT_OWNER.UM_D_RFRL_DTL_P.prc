CREATE OR REPLACE PROCEDURE             "UM_D_RFRL_DTL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- James Doucette
--
-- Loads stage table UM_D_RFRL_DTL from stage table PS_UM_REFL_DTL_TBL.
--
-- V01  SMT-xxxx 2/14/2019,    James Doucette
--                             Converted from DataStage
----V02 2/12/2021            -- Srikanth,Pabbu made changes to RFRL_DTL_SID field
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_RFRL_DTL';
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

strMessage01    := 'Merging data into CSMRT_OWNER.UM_D_RFRL_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.UM_D_RFRL_DTL';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.UM_D_RFRL_DTL T 
using (                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  with Q1 as (  
select INSTITUTION INSTITUTION_CD, UM_REFRL_GRP RFRL_GRP, UM_REFRL_DTL, SRC_SYS_ID, 
       UM_REFRL_DATE RFRL_DATE, STATUS RFRL_DTL_STATUS, DESCR, 
       DATA_ORIGIN 
  from CSSTG_OWNER.PS_UM_REFL_DTL_TBL),
       S as (
select Q1.INSTITUTION_CD, Q1.RFRL_GRP, Q1.UM_REFRL_DTL, Q1.SRC_SYS_ID, 
       Q1.RFRL_DATE, Q1.DESCR, 
       nvl(G1.RFRL_GRP_SID,2147483646) RFRL_GRP_SID, 
       Q1.RFRL_DTL_STATUS,  
       Q1.DATA_ORIGIN  
  from Q1
  left outer join UM_D_RFRL_GRP G1      -- Temp!!!  
    on Q1.INSTITUTION_CD = G1.INSTITUTION_CD
   and Q1.RFRL_GRP = G1.RFRL_GRP 
   and Q1.SRC_SYS_ID = G1.SRC_SYS_ID
   and G1.DATA_ORIGIN <> 'D')
   
select nvl(D.RFRL_DTL_SID, --max(D.RFRL_DTL_SID) over (partition by 1) +  This code does not ignore SID 2147483646 and below line will fix the issue and is added on 2/12/2021
  (select nvl(max(RFRL_DTL_SID),0) from CSMRT_OWNER.UM_D_RFRL_DTL where RFRL_DTL_SID <> 2147483646) +                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
       row_number() over (partition by 1 order by D.RFRL_DTL_SID nulls first)) RFRL_DTL_SID, 
       nvl(D.INSTITUTION_CD, S.INSTITUTION_CD) INSTITUTION_CD, 
       nvl(D.RFRL_GRP, S.RFRL_GRP) RFRL_GRP, 
       nvl(D.RFRL_DTL, S.UM_REFRL_DTL) RFRL_DTL, 
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID, 
       decode(D.DESCR, S.DESCR, D.DESCR, S.DESCR) DESCR, 
       decode(D.RFRL_DATE, S.RFRL_DATE, D.RFRL_DATE, S.RFRL_DATE) RFRL_DATE, 
       decode(D.RFRL_GRP_SID, S.RFRL_GRP_SID, D.RFRL_GRP_SID, S.RFRL_GRP_SID) RFRL_GRP_SID, 
       decode(D.RFRL_DTL_STATUS, S.RFRL_DTL_STATUS, D.RFRL_DTL_STATUS, S.RFRL_DTL_STATUS) RFRL_DTL_STATUS,  
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN
  from S
left outer join CSMRT_OWNER.UM_D_RFRL_DTL D  
   on D.RFRL_DTL_SID <> 2147483646
  and D.INSTITUTION_CD = S.INSTITUTION_CD
  and D.RFRL_GRP = S.RFRL_GRP 
  and D.RFRL_DTL = S.UM_REFRL_DTL
  and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
) S                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
   on (T.INSTITUTION_CD = S.INSTITUTION_CD
  and T.RFRL_GRP = S.RFRL_GRP 
  and T.RFRL_DTL = S.RFRL_DTL
  and T.SRC_SYS_ID = S.SRC_SYS_ID )                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
 when matched then update set                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       T.DESCR = S.DESCR,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       T.RFRL_DATE = S.RFRL_DATE,
       T.RFRL_GRP_SID = S.RFRL_GRP_SID,
       T.RFRL_DTL_STATUS = S.RFRL_DTL_STATUS,   
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
 where                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
       decode(T.DESCR,S.DESCR,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       decode(T.RFRL_DATE,S.RFRL_DATE,0,1) = 1 or
       decode(T.RFRL_GRP_SID,S.RFRL_GRP_SID,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       decode(T.RFRL_DTL_STATUS,S.RFRL_DTL_STATUS,0,1) = 1 or 
	   decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1
  when not matched then                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
insert (                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       T.RFRL_DTL_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       T.INSTITUTION_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
       T.RFRL_GRP,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
       T.RFRL_DTL,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       T.SRC_SYS_ID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       T.DESCR,
	   T.RFRL_DATE,
	   T.RFRL_GRP_SID,
	   T.RFRL_DTL_STATUS,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       T.DATA_ORIGIN, 
       T.CREATED_EW_DTTM, 
       T.LASTUPD_EW_DTTM
	   )                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
values (                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       S.RFRL_DTL_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       S.INSTITUTION_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
       S.RFRL_GRP,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
       S.RFRL_DTL,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       S.SRC_SYS_ID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       S.DESCR,
	   S.RFRL_DATE,
	   S.RFRL_GRP_SID,
	   S.RFRL_DTL_STATUS,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
       S.DATA_ORIGIN,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
       SYSDATE,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       SYSDATE)
;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_RFRL_DTL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_RFRL_DTL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.UM_D_RFRL_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.UM_D_RFRL_DTL';
update CSMRT_OWNER.UM_D_RFRL_DTL T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.RFRL_DTL_SID <> 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PS_UM_REFL_DTL_TBL S  
                    where T.INSTITUTION_CD = S.INSTITUTION
                      and T.RFRL_GRP = S.UM_REFRL_GRP
					  and T.RFRL_DTL = S.UM_REFRL_DTL
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_RFRL_DTL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_RFRL_DTL',
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

END UM_D_RFRL_DTL_P;
/
