SET @HashedAccount = CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', @AccountNumber), 2);
