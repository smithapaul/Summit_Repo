DROP FUNCTION CSMRT_OWNER.GET_SR_CLASSES_PERSON_TERM
/

--
-- GET_SR_CLASSES_PERSON_TERM  (Function) 
--
CREATE OR REPLACE FUNCTION CSMRT_OWNER.GET_SR_CLASSES_PERSON_TERM
(
IN_PERSON_ID IN VARCHAR2, 
IN_TERM_LD IN VARCHAR2, 
IN_INSTITUTION IN VARCHAR2,
IN_CRSE_CD IN VARCHAR2
) 
RETURN VARCHAR2 IS RET_NAME VARCHAR2(2000);
strClassDisplayNames        varchar2(2000);
strCourseCDs                varchar2(256);
strFirstClass               char(1);
                    
CURSOR c1
IS
    SELECT c.SBJCT_CD || '.' || c.CATALOG_NBR || '.'|| c.CLASS_SECTION_CD || '.' || c.DESCR as "Class Display Name"
      INTO RET_NAME       
      FROM UM_D_PERSON_CS_VW p, UM_F_STDNT_ENRL e, UM_D_TERM_VW t, PS_D_ENRLMT_STAT s, UM_D_CLASS_VW c, UM_D_CRSE_VW cr
      WHERE p.PERSON_ID = IN_PERSON_ID
        and p.PERSON_SID = e.PERSON_SID
        and t.TERM_LD = IN_TERM_LD 
        and e.TERM_SID = t.TERM_SID
        and e.ENRLMT_STAT_SID = s.ENRLMT_STAT_SID
        and s.ENRLMT_STAT_ID = 'E'
        and e.CLASS_SID = c.CLASS_SID
        and c.CRSE_SID = cr.CRSE_SID
        --and CR.CRSE_CD IN (strCourseCDs)
        and e.INSTITUTION_CD = IN_INSTITUTION;  
BEGIN

      strFirstClass := 'Y';
      /*
      If IN_CRSE_CD != 'All'
      Then
        strCourseCDs := REPLACE(IN_CRSE_CD, ',',''',''');
        strCourseCDs := ''''||strCourseCDs||'''';
        dbms_output.put_line(strCourseCDs);
      End If;
      */
      
      OPEN c1;
      loop
          FETCH c1 INTO RET_NAME;
          exit when c1%notfound;
          If strFirstClass = 'Y' then
          strClassDisplayNames := RET_NAME;
          strFirstClass := 'N';
          else
          strClassDisplayNames := strClassDisplayNames||'*'||RET_NAME;
          End If;
          
          --dbms_output.put_line(RET_NAME);
          dbms_output.put_line(strClassDisplayNames);
      end loop;
      CLOSE c1;

      RET_NAME := strClassDisplayNames; 

  RETURN RET_NAME;        
END GET_SR_CLASSES_PERSON_TERM;
/
