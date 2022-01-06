CREATE OR REPLACE PROCEDURE             "PS_D_ACAD_LOAD_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_ACAD_LOAD from PeopleSoft table PSXLATITEM.
--
 --V01  SMT-xxxx 10/12/2017,    James Doucette
--                              Converted from DataStage
-- V02 2/11/2021            -- Srikanth,Pabbu made changes to ACAD_LOAD_SID field
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_ACAD_LOAD';
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


strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_ACAD_LOAD';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_ACAD_LOAD';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_ACAD_LOAD T                                                                                                                                                                                                
using (                                                                                                                                                                                                                                                         
  with X as (  
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) XLAT_ORDER
  from CSSTG_OWNER.PSXLATITEM
),
       S as (
select 'N' APPRVD_IND, FIELDVALUE ACAD_LOAD_CD, SRC_SYS_ID, 
       XLATSHORTNAME ACAD_LOAD_SD, XLATLONGNAME ACAD_LOAD_LD, DATA_ORIGIN 
  from X 
 where FIELDNAME = 'ACADEMIC_LOAD' 
   and XLAT_ORDER = 1
 union all
select 'Y' APPRVD_IND, FIELDVALUE ACAD_LOAD_CD, SRC_SYS_ID, 
       XLATSHORTNAME ACAD_LOAD_SD, XLATLONGNAME ACAD_LOAD_LD, DATA_ORIGIN 
  from X 
 where FIELDNAME = 'ACAD_LOAD_APPR' 
   and XLAT_ORDER = 1)                                                                                                                                                                                              
select nvl(D.ACAD_LOAD_SID, --max(D.ACAD_LOAD_SID) over (partition by 1) +  this code does not ignore SID 2147483646 and below line will fix the issue and is added on 2/11/2021
 (select nvl(max(ACAD_LOAD_SID),0) from CSMRT_OWNER.PS_D_ACAD_LOAD where ACAD_LOAD_SID <> 2147483646) +                                                                                                                                                                                      
       row_number() over (partition by 1 order by D.ACAD_LOAD_SID nulls first)) ACAD_LOAD_SID,                                                                                                                                                                  
       nvl(D.APPRVD_IND, S.APPRVD_IND) APPRVD_IND,                                                                                                                                                                                                              
       nvl(D.ACAD_LOAD_CD, S.ACAD_LOAD_CD) ACAD_LOAD_CD,                                                                                                                                                                                                        
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,                                                                                                                                                                                                              
       decode(D.ACAD_LOAD_SD, S.ACAD_LOAD_SD, D.ACAD_LOAD_SD, S.ACAD_LOAD_SD) ACAD_LOAD_SD,                                                                                                                                                                     
       decode(D.ACAD_LOAD_LD, S.ACAD_LOAD_LD, D.ACAD_LOAD_LD, S.ACAD_LOAD_LD) ACAD_LOAD_LD,                                                                                                                                                                     
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,                                                                                                                                                                          
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,                                                                                                                                                                                                         
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM                                                                                                                                                                                                          
  from S                                                                                                                                                                                                                                                        
  left outer join CSMRT_OWNER.PS_D_ACAD_LOAD D                                                                                                                                                                                                              
    on D.ACAD_LOAD_SID <> 2147483646                                                                                                                                                                                                                            
   and D.APPRVD_IND = S.APPRVD_IND                                                                                                                                                                                                                              
   and D.ACAD_LOAD_CD = S.ACAD_LOAD_CD                                                                                                                                                                                                                          
   and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                              
) S                                                                                                                                                                                                                                                             
    on  (T.APPRVD_IND = S.APPRVD_IND                                                                                                                                                                                                                            
   and  T.ACAD_LOAD_CD = S.ACAD_LOAD_CD                                                                                                                                                                                                                         
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                            
 when matched then update set                                                                                                                                                                                                                                   
       T.ACAD_LOAD_SD = S.ACAD_LOAD_SD,                                                                                                                                                                                                                         
       T.ACAD_LOAD_LD = S.ACAD_LOAD_LD,                                                                                                                                                                                                                         
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                           
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                              
 where                                                                                                                                                                                                                                                          
       decode(T.ACAD_LOAD_SD,S.ACAD_LOAD_SD,0,1) = 1 or                                                                                                                                                                                                         
       decode(T.ACAD_LOAD_LD,S.ACAD_LOAD_LD,0,1) = 1 or                                                                                                                                                                                                         
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1                                                                                                                                                                                                              
  when not matched then                                                                                                                                                                                                                                         
insert (                                                                                                                                                                                                                                                        
       T.ACAD_LOAD_SID,                                                                                                                                                                                                                                         
       T.APPRVD_IND,                                                                                                                                                                                                                                            
       T.ACAD_LOAD_CD,                                                                                                                                                                                                                                          
       T.SRC_SYS_ID,                                                                                                                                                                                                                                            
       T.ACAD_LOAD_SD,                                                                                                                                                                                                                                          
       T.ACAD_LOAD_LD,                                                                                                                                                                                                                                          
       T.DATA_ORIGIN,                                                                                                                                                                                                                                           
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                       
       T.LASTUPD_EW_DTTM)                                                                                                                                                                                                                                       
values (                                                                                                                                                                                                                                                        
       S.ACAD_LOAD_SID,                                                                                                                                                                                                                                         
       S.APPRVD_IND,                                                                                                                                                                                                                                            
       S.ACAD_LOAD_CD,                                                                                                                                                                                                                                          
       S.SRC_SYS_ID,                                                                                                                                                                                                                                            
       S.ACAD_LOAD_SD,                                                                                                                                                                                                                                          
       S.ACAD_LOAD_LD,                                                                                                                                                                                                                                          
       S.DATA_ORIGIN,                                                                                                                                                                                                                                           
       SYSDATE,                                                                                                                                                                                                                                                 
       SYSDATE)
;


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_ACAD_LOAD rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_ACAD_LOAD',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_ACAD_LOAD';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

update CSMRT_OWNER.PS_D_ACAD_LOAD T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.ACAD_LOAD_SID < 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PSXLATITEM S
                    where S.FIELDNAME = 'ACADEMIC_LOAD'
                      and T.APPRVD_IND = 'N'
                      and T.ACAD_LOAD_CD = S.FIELDVALUE
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
					  and S.DATA_ORIGIN <> 'D')
   and not exists (select 1
                     from CSSTG_OWNER.PSXLATITEM S
                    where S.FIELDNAME = 'ACAD_LOAD_APPR'
                      and T.APPRVD_IND = 'Y'
                      and T.ACAD_LOAD_CD = S.FIELDVALUE
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
					  and S.DATA_ORIGIN <> 'D')
;	

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_ACAD_LOAD rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_ACAD_LOAD',
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

END PS_D_ACAD_LOAD_P;
/
