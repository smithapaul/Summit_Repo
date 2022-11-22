DROP PROCEDURE CSMRT_OWNER.GEN_DELETE_STATEMENT
/

--
-- GEN_DELETE_STATEMENT  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.GEN_DELETE_STATEMENT 
  AUTHID CURRENT_USER
IS
   TEMP1                   VARCHAR2(20) :='00000000';
   TEMP2                   VARCHAR2(20)  :='99999999';
   intInsertCount           Integer :=0;  
   CURSOR del_cur                 
   IS
       select OWNER,TABLE_NAME,COLUMN_NAME  from CSSTG_OWNER.TEMP_EMPLID_ISSUE_TABLE                
      ;
       CURSOR cnt_cur                 
   IS
       select OWNER,TABLE_NAME,COLUMN_NAME  from CSSTG_OWNER.TEMP_EMPLID_ISSUE_TABLE                
      ;
   p_del_rec             del_cur%ROWTYPE;
   p_cnt_rec             cnt_cur%ROWTYPE;
   p_no_of_rows      INTEGER;
   v_ref_cur_query   VARCHAR2 (16000);
    v_ref_count_cur_query   VARCHAR2 (16000);
   TGT_TBL           VARCHAR2 (16000);
BEGIN

   OPEN del_cur;
   LOOP
      FETCH del_cur INTO p_del_rec;
      EXIT WHEN del_cur%NOTFOUND;
         v_ref_cur_query :=
         'DELETE FROM '
      || p_del_rec.OWNER
      || '.'
      || p_del_rec.TABLE_NAME
      || ' where not(length('
      ||p_del_rec.COLUMN_NAME
      ||')'
      ||' = 8 and '
      || p_del_rec.COLUMN_NAME
      || ' between '
      ||''''
      || TEMP1 
      ||''''
      || ' and ' 
       ||''''
      || TEMP2
       ||''''
      ||');';
   DBMS_OUTPUT.put_line (v_ref_cur_query);
   intInsertCount  := intInsertCount + 1;
   --p_no_of_rows := SQL%ROWCOUNT;
     --DBMS_OUTPUT.put_line ('total sql row count '||p_no_of_rows);
    END LOOP;   
  DBMS_OUTPUT.put_line ('total sql row count '||intInsertCount);
  
OPEN cnt_cur;
   LOOP
      FETCH cnt_cur INTO p_cnt_rec;
      EXIT WHEN cnt_cur%NOTFOUND;
         v_ref_count_cur_query :=
         'select count(*), '
      || ''''
      ||p_cnt_rec.TABLE_NAME
      ||''''
      || ' from '  
      || p_cnt_rec.OWNER
      || '.'
      || p_cnt_rec.TABLE_NAME
      || ' where not(length('
      ||p_cnt_rec.COLUMN_NAME
      ||')'
      ||' = 8 and '
      || p_cnt_rec.COLUMN_NAME
      || ' between '
      ||''''
      || TEMP1 
      ||''''
      || ' and ' 
       ||''''
      || TEMP2
       ||''''
      ||')'
      ||' UNION ';
   DBMS_OUTPUT.put_line (v_ref_count_cur_query);
  -- intInsertCount  := intInsertCount + 1;
   --p_no_of_rows := SQL%ROWCOUNT;
     --DBMS_OUTPUT.put_line ('total sql row count '||p_no_of_rows);
    END LOOP;   
  
    --  DBMS_OUTPUT.PUT_LINE ('-- LOGINID ' || p_login_rec.LOGINID);




/*

   TGT_col_str       VARCHAR2 (16000);
   v_ref_cur_query   VARCHAR2 (16000);
   v_code            NUMBER;
   v_errm            VARCHAR2 (1000);
   p_no_of_rows      PLS_INTEGER;
BEGIN
   FOR rec
      IN (
  select TABLE_OWNER,TABLE_NAME,COLUMN_NAME  from DBA_IND_COLUMNS C where TABLE_OWNER in ('CSSTG_OWNER') and COLUMN_NAME IN ( 'COMMON_ID','EMPLID')
      )
   LOOP
     v_ref_cur_query :=
         'DELETE FROM '
      || REC.OWNER
      || '.'
      || REC.TABLE_NAME
      || ' where not(length('
      ||REC.COLUMN_NAME
      ||')'
      ||' = 8 and '
      || REC.COLUMN_NAME
      || ' between '
      ||''''
      || TEMP1 
      ||''''
      || ' and ' 
       ||''''
      || TEMP2
       ||''''
      ||');';
   DBMS_OUTPUT.put_line (v_ref_cur_query);

   END LOOP;
  -- EXECUTE IMMEDIATE v_ref_cur_query;
*/


EXCEPTION
   WHEN OTHERS
   THEN
      DBMS_OUTPUT.PUT_LINE ('Invalid table: ' || SQLERRM);

END;
/
