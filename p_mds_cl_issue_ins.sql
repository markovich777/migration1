SET quoted_identifier off
SET ansi_warnings off
SET ansi_padding off
SET ansi_nulls off
SET concat_null_yields_null off
go

IF OBJECT_ID('dbo.p_mds_cl_issue_ins') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.p_mds_cl_issue_ins
    IF OBJECT_ID('dbo.p_mds_cl_issue_ins') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.p_mds_cl_issue_ins >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.p_mds_cl_issue_ins >>>'
END
go

/*
* $Log: /Dalcomp/Database/SqlServer/MDS/calendar/p_mds_cl_issue_ins.sql $
* 
*   674931  10/22/2013 3:54P  buslovig  
* Add Kroll                                                                                            
* 
* 11    8/03/11 12:15p Buslovig
* Added ratings
* 
* 10    6/16/11 12:08p Buslovig
* Task #54678 Move fields to Series page 
* 
* 9     6/01/11 9:02a Buslovig
* Added ratings
* 
* 8     3/01/11 2:10p Buslovig
* 
* 5     2/23/11 12:49p Buslovig
* Linking project release
* 
* 4     4/27/10 4:33p Buslovig
* SQL 2005 changes
* 
* 3     4/21/09 1:02p Buslovig
* 
* 2     4/15/09 4:44p Buslovig
* 
* 1     3/05/09 3:51p Buslovig
* created
*/
--p_mds_cl_issue_ins 160698,1,'n','p',2047498791

CREATE PROCEDURE dbo.p_mds_cl_issue_ins 
	
	@al_issue_key_no		INTEGER,
	@al_mult_series_count	INTEGER,
	@as_offering_type		CHAR(2),
	@as_orig_appl_cd		CHAR(1),
	@al_issue_id			INTEGER
	  
AS
BEGIN
	SET NOCOUNT ON

	DECLARE @ls_cust_id CHAR(16)
	DECLARE @ll_user_id INTEGER
	DECLARE @ls_msg VARCHAR(255)
	DECLARE @ll_series_id INTEGER
	DECLARE @adtm_first_due_date DATETIME
	DECLARE @adtm_last_maturity_date DATETIME
	DECLARE @adtm_frequency DATETIME
	DECLARE @sCustID  CHAR(16)
	DECLARE @dNumBonds DECIMAL (18,5)
	DECLARE @global_issue_id UNIQUEIDENTIFIER




	SELECT @ll_user_id = user_id,
		@ls_cust_id = cust_id
	FROM s_user (NOLOCK)
	WHERE user_name = SUSER_SNAME()
		AND (app_name = 'MDS' OR user_type = 'AD')

	IF @@error <> 0 OR @@rowcount = 0
	BEGIN
		RAISERROR('User is not in s_user table.', 16, 1)        RETURN
	END

	IF @al_issue_key_no < 1
	BEGIN
		RAISERROR('Parameter @al_issue_key_no is not valid.', 16, 1)        RETURN
	END

	IF @al_mult_series_count < 1
	BEGIN
		RAISERROR('Parameter @al_mult_series_count is not valid.', 16, 1)        RETURN
	END

	IF @as_offering_type = 'C' and @al_issue_key_no > 0
		BEGIN
			RETURN
		END
		

		SELECT @ll_series_id = 1

		WHILE @ll_series_id <= @al_mult_series_count
		BEGIN
			IF @as_offering_type = 'N' 
				BEGIN
					UPDATE s
					SET s.link_series_id = ss.series_id    
						,s.postsale_moody = ss.moodys_rating_cd
						,s.postsale_sp = ss.sp_rating_cd
						,s.postsale_fitch = ss.fitch_rating_cd
						,s.moody_und =  ss.moodys_und_rating_cd
						,s.sp_und = ss.sp_und_rating_cd
						,s.fitch_und = ss.fitch_und_rating_cd   
						,s.delivery_date = ss.delivery_dt
						,s.delivery_location = ss.delivery_place_txt
						,s.dated_date = ss.dated_dt	
						,s.long_description_1 = ISNULL(s.long_description_1, ss.series_desc)
						,s.capital_type = ISNULL(s.capital_type, ss.mds_issue_type_cd)
						,s.tax_status_type = ss.taxable_ind
						,s.series_ins_type = ss.insurer_id
						,s.first_int_date = ss.first_coupon_dt
						,s.int_accrue_date = ISNULL(s.int_accrue_date, ss.int_accrue_dt)
						,s.coupon_type = CASE ss.coupon_type_cd WHEN 'Zero Coupon' THEN 'C' ELSE 'X' END
					FROM brs_series s
					JOIN brs_stg_series ss ON s.issue_key_no = @al_issue_key_no and s.series_id = @ll_series_id
					WHERE ss.issue_id = @al_issue_id
					AND ss.mds_series_id = @ll_series_id

					UPDATE s
					SET s.bank_qualified_type = ISNULL(s.bank_qualified_type, ss.bank_qualified),
						s.series_state = ISNULL(s.series_state, ss.bookmgr_state_cd)
					FROM brs_series s
					JOIN brs_stg_issue ss ON s.issue_key_no = @al_issue_key_no and s.series_id = @ll_series_id
					WHERE ss.issue_id = @al_issue_id
					


					IF @@error <> 0 
					BEGIN
						RAISERROR('Update of brs_series table failed. (p_mds_cl_issue_ins)', 16, 1)        RETURN
					END

			IF	(SELECT COUNT(*)
				FROM brs_stg_maturity 
				WHERE issue_id = @al_issue_id AND mds_series_seq = @ll_series_id) > 0
					
				BEGIN

					DELETE brs_maturity WHERE issue_key_no = @al_issue_key_no AND series_id = @ll_series_id
			
					INSERT INTO brs_maturity
							   (issue_key_no
							   ,series_id
							   ,maturity_seq
							   ,amount
							   ,maturity_date
							   ,cusip9
							   ,postsale_moody
							   ,postsale_sp
							   ,postsale_fitch
							   ,day_count_type
							   )
					SELECT 
							   @al_issue_key_no	,
								@ll_series_id	,
								mds_maturity_seq ,
								number_bonds = CASE WHEN ISNULL(number_bonds, 0) = 0 THEN NULL ELSE (number_bonds * 1000) END,								
								CONVERT(datetime,maturity_dt)	,
								cusip,
								or_moodys_rating_id,
								or_sp_rating_id,
								or_fitch_rating_id,
								'1'
					FROM brs_stg_maturity
					WHERE issue_id = @al_issue_id
						 AND mds_series_seq = @ll_series_id
					
					IF @@error <> 0 
					BEGIN
						RAISERROR('Update of brs_maturity table failed. (p_mds_cl_issue_ins)', 16, 1)        RETURN
					END

					UPDATE m
					SET 	m.postsale_moody =	s.postsale_moody ,
							m.postsale_sp =	s.postsale_sp,
							m.postsale_fitch =	 s.postsale_fitch,
							m.moody =	s.moody_und,
							m.sp =	s.sp_und,
							m.fitch =	s.fitch_und,
							m.postsale_insurance_type = s.series_ins_type,
							m.taxable_ind = CASE s.tax_status_type WHEN 'E' THEN 'N' WHEN 'A' THEN 'N' WHEN 'T' THEN 'Y' ELSE taxable_ind END 
					FROM  brs_maturity m
					JOIN  brs_series s ON m.issue_key_no = s.issue_key_no and m.series_id = s.series_id
					WHERE m.issue_key_no = @al_issue_key_no and m.series_id = @ll_series_id


					SELECT @adtm_first_due_date = MIN(maturity_dt) FROM brs_stg_maturity WHERE issue_id = @al_issue_id AND mds_series_seq = @ll_series_id

					SELECT @adtm_last_maturity_date = MAX(maturity_dt)FROM brs_stg_maturity WHERE issue_id = @al_issue_id AND mds_series_seq = @ll_series_id
					
					SELECT @adtm_frequency = (select maturity_dt from brs_stg_maturity where issue_id = @al_issue_id and mds_series_seq = @ll_series_id and mds_maturity_seq = 2)

					UPDATE s
					SET s.first_due_date = @adtm_first_due_date,
						s.last_maturity_date = @adtm_last_maturity_date,
						s.frequency = CASE WHEN datediff (month, @adtm_frequency, @adtm_first_due_date) > 6 THEN 2 ELSE 1 END
					FROM brs_series s
					WHERE s.issue_key_no = @al_issue_key_no
						AND s.series_id = @ll_series_id

					IF EXISTS (SELECT sertrm_id FROM brs_stg_maturity WHERE sertrm_id = 2 AND issue_id = @al_issue_id AND mds_series_seq = @ll_series_id )
						BEGIN 
							UPDATE brs_series SET term_type = '02' WHERE issue_key_no = @al_issue_key_no AND series_id = @ll_series_id
						END
				END

			IF	(SELECT count(*) FROM brs_stg_series 
					WHERE issue_id = @al_issue_id
					AND mds_series_seq = @ll_series_id
					AND use_proceeds_cd IS NOT NULL) > 0 AND @al_issue_key_no > 0
				
				BEGIN
					DELETE brs_use_of_proceeds_type WHERE issue_key_no = @al_issue_key_no AND series_id = @ll_series_id
				
					INSERT INTO brs_use_of_proceeds_type 
					SELECT @al_issue_key_no, mds_series_seq, 1, use_proceeds_cd
					FROM brs_stg_series 
					WHERE issue_id = @al_issue_id
					AND mds_series_seq = @ll_series_id
					AND use_proceeds_cd IS NOT NULL
				END

			IF  (SELECT count(*) 
					 FROM brs_stg_call_feature_type c
					 JOIN brs_stg_series s ON c.series_id = s.series_id
					 WHERE s.issue_id = @al_issue_id ) > 0 
				BEGIN
					DELETE brs_call_feature_type WHERE issue_key_no = @al_issue_key_no AND series_id = @ll_series_id
				
						INSERT INTO	brs_call_feature_type
							(issue_key_no,
							series_id,
							call_id,
							call_type,
							call_date,
							call_price,
							par_call_date
							)
						SELECT 
							@al_issue_key_no,
							@ll_series_id,
							1,
							CASE mds_call_feature_type_cd
										WHEN 'Any time at par' THEN '09'
										WHEN 'Make Whole Call' THEN '10'
										WHEN 'Non Callable' THEN '04'
										WHEN 'Not Callable' THEN '04'
										WHEN 'Callable' THEN '03'
										WHEN 'Callable Anytime' THEN '09'
										WHEN 'Pyramid' THEN '08' 
										WHEN 'At par' THEN '06' 
										WHEN 'Declining to par' THEN '01' 
										ELSE null 
										END,
							first_call_dt,
							first_call_price,
							last_call_dt
						FROM brs_stg_call_feature_type 
						JOIN brs_stg_series ON brs_stg_call_feature_type.series_id = brs_stg_series.series_id
						WHERE brs_stg_series.issue_id = @al_issue_id
						AND brs_stg_series.mds_series_id = @ll_series_id
				END

				SELECT @dNumBonds = SUM(ISNULL(number_bonds,0) * 1000)
				FROM brs_stg_maturity  
				WHERE brs_stg_maturity.issue_id = @al_issue_id AND mds_series_seq = @ll_series_id 

				UPDATE brs_series 
				SET series_amount = @dNumBonds
				WHERE issue_key_no = @al_issue_key_no AND series_id = @ll_series_id 
																

				SELECT @ll_series_id = @ll_series_id + 1
			END

		END


		IF (SELECT COUNT(1) FROM brs_agent WHERE dal_uwcode = @sCustID) > 0
			SELECT @sCustID = agent_id  
			FROM brs_agent 
			JOIN brs_stg_issue on brs_agent.dal_uwcode = brs_stg_issue.bookmgr_MUMPS_mnemonic 
			WHERE brs_stg_issue.bookmgr_state_cd = brs_agent.state
			AND brs_stg_issue.issue_id = @al_issue_id
			
		ELSE
			SELECT @sCustID = 'TEST-1'


	
		
		IF  @as_offering_type = 'N' and @sCustID IS NOT NULL AND @al_issue_key_no > 0
--		 AND (NOT EXISTS (SELECT 1 FROM brs_bid WHERE issue_key_no = @al_issue_key_no AND cust_id = @sCustID))
		 AND (SELECT count(*) FROM brs_maturity WHERE issue_key_no = @al_issue_key_no) > 0

			BEGIN

			EXECUTE p_gen_delete_bids @al_issue_key_no , NULL , NULL

			EXECUTE p_mds_bidders_upd 'I', @al_issue_key_no, NULL, @sCustID, NULL, NULL, NULL, 'N', 'Y', 'Y', 'Y', NULL,NULL
				IF @@error <> 0 
				BEGIN
					RAISERROR('p_mds_bidders_upd failed.', 16, 1)        RETURN
				END
			EXECUTE p_mds_create_syndicate @al_issue_key_no , @sCustID
				IF @@error <> 0 OR @@rowcount = 0
				BEGIN
					RAISERROR('p_mds_create_syndicate failed.', 16, 1)        RETURN
				END
			END
						UPDATE b
						SET		b.bid_amount = CASE WHEN ISNULL(tm.number_bonds, 0) = 0 THEN NULL
									ELSE (ISNULL( tm.sinking_amt,tm.number_bonds) * 1000) END,
								b.coupon = tm.coupon,
								b.dollar_price = tm.dollar_price,
								b.yield = tm.yield_price,
								b.nro_flag = tm.nro_ind,
								b.sealed_bid_flag = tm.sealed_bid_ind,
								b.term_year = CASE tm.sinking_amt WHEN NULL THEN NULL ELSE maturity_dt END,
								b.serial_term_flag = CASE tm.sertrm_id WHEN '1' THEN 'N' WHEN '2' THEN 'Y' END,
								b.price_indic = CASE  WHEN list_unit_type_id = 2 THEN 1 WHEN list_unit_type_id = 1 THEN 0 END
						FROM brs_bid b
						LEFT JOIN brs_stg_maturity tm ON b.series_id = tm.mds_series_seq
							AND b.maturity_seq = tm.mds_maturity_seq
						WHERE b.issue_key_no = @al_issue_key_no
							AND b.cust_id = @sCustID
							AND tm.issue_id = @al_issue_id

						UPDATE b  
						SET b.term_year = tb.maturity_dt
						FROM brs_bid b
						JOIN brs_stg_maturity tm ON b.series_id = tm.mds_series_seq
							AND b.maturity_seq = tm.mds_maturity_seq
						JOIN brs_stg_term_bonds tb ON tm.maturity_id = tb.maturity_id
							AND tb.sinking_dt = tm.maturity_dt
						WHERE b.issue_key_no = @al_issue_key_no
							AND b.cust_id = @sCustID
							AND tm.issue_id = @al_issue_id
							
						UPDATE m
						SET m.amount = b.bid_amount	, m.term_year = b.term_year
						FROM brs_maturity m
						JOIN brs_bid b ON m.issue_key_no = b.issue_key_no and m.maturity_seq = b.maturity_seq AND m.series_id = b.series_id
						WHERE m.issue_key_no = @al_issue_key_no
							AND b.cust_id = @sCustID


--						IF @@error <> 0 OR @@rowcount = 0
--						BEGIN
--							RAISERROR('Auto Update brs_bid table failed.', 16, 1)        --							RETURN
--						END




	UPDATE brs_stg_issue SET issue_key_no = @al_issue_key_no WHERE issue_id = @al_issue_id
	UPDATE brs_stg_series SET mds_issue_key_no = @al_issue_key_no, linked_ind  = 'L' WHERE issue_id = @al_issue_id
	UPDATE brs_stg_maturity SET issue_key_no = @al_issue_key_no WHERE issue_id = @al_issue_id
	IF EXISTS (SELECT COUNT(*) FROM brs_issue WHERE  issue_id = @al_issue_id AND issue_key_no <> @al_issue_key_no)
	BEGIN
		UPDATE brs_issue SET issue_id = NULL, global_issue_id = NULL WHERE  issue_id = @al_issue_id AND issue_key_no <> @al_issue_key_no
	END
	

	UPDATE brs_issue SET amount = (SELECT SUM(ISNULL(series_amount,0))
									FROM brs_series 
									WHERE issue_key_no = @al_issue_key_no)
	WHERE issue_key_no = @al_issue_key_no

	UPDATE brs_issue SET public_view_flag = 'N',
						bidcomp_view_flag = 'N',
						maint_id = REPLACE (SUSER_SNAME(), 'MFIDEAL\', ''),
						maint_dte = GETDATE()
	WHERE issue_key_no = @al_issue_key_no

	SET @global_issue_id = (SELECT global_issue_id FROM brs_stg_issue WHERE issue_id = @al_issue_id)
	IF @global_issue_id IS NOT NULL
		BEGIN
			UPDATE brs_issue SET global_issue_id = @global_issue_id WHERE issue_key_no = @al_issue_key_no
		END

	IF EXISTS (SELECT COUNT(*) 
				FROM brs_series s, brs_stg_series ss	
				WHERE s.link_series_id = ss.series_id
				AND ss.issue_id = @al_issue_id
				AND s.issue_key_no <> @al_issue_key_no)
	BEGIN
		UPDATE s SET s.link_series_id = NULL 
		FROM brs_series s, brs_stg_series ss	
				WHERE s.link_series_id = ss.series_id
				AND ss.issue_id = @al_issue_id
				AND s.issue_key_no <> @al_issue_key_no
	END 

	
END 
go
IF OBJECT_ID('dbo.p_mds_cl_issue_ins') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.p_mds_cl_issue_ins >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.p_mds_cl_issue_ins >>>'
go
GRANT EXECUTE ON dbo.p_mds_cl_issue_ins TO brs, CASA
go

