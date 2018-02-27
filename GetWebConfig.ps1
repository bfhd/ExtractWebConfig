<#
.Synopsis
   Extract Web configuration from Titanium database and generate SQL so it can be inserted somewhere else.
.DESCRIPTION
   Convert existing Web configuration into a sql merge script so it can be copied to another database.

.PARAMETER ServerInstance
    The database server instance name. Defaults to local server.
.PARAMETER Database
    The database name.
.PARAMETER OutputFile
    The name of the file that the SQL mappings will be stored in (defaults to "DatabaseName-Web-Mappings-<Date>.sql").
.PARAMETER DatabaseUser
    The database user (if using SQL authentication). If not specified, will use integrated authentication.
.PARAMETER DatabasePassword
    The database password (if using SQL authentication).

.EXAMPLE
   
   GetWebMappings -ServerInstance MyServer -Database MyDB -OutputFile MyMappings.sql -DatabaseUser MyUser -DatabasePassword MyPassword

.INPUTS
   -ServerInstance <string>
   The SQL Server instance name.

   -Database <string>
   The target SQL Server database.

   -DatabaseUser
   The database user (if using SQL authentication). If not specified, will use integrated authentication.

   -DatabasePassword
   The database passowrd (if using SQL authentication).

   -OutputFile <string>
   File name to save the SQL script into (will clobber existing files) - defaults to "DatabaseName-Web-Mappings-<Date>.sql".
	
   -Query <string> (only for ConvertQueryDataToSQL)
   select statement that specifies the columns of configuration to be extracted (usually select *)
	
.OUTPUTS
   A .sql file containing insert statements for the specified Web configuration (will clobber existing files).
.NOTES
   Author: Ben Roper, 2018/02/26   
   Modified version of https://www.mssqltips.com/sqlservertip/4287/generate-insert-scripts-from-sql-server-queries-and-stored-procedure-output/
   Requires:
    Powershell version 2.0+
	
   
#>
[CmdletBinding()]
Param
(
	# SQL Server instance name, defaults to local server
	[Parameter(Mandatory=$true, 
	           Position=0)]
	[Alias("S")] 
	[string]$ServerInstance=$env:ComputerName,

	# Database Name
	[Parameter(Mandatory=$false,
	           Position=1)]
    [Alias("D")]
	[AllowEmptyCollection()]
	[string] $Database='Titanium',

    # Output file name
	[Parameter(Mandatory=$false,
               Position=2)]
    [Alias("O")]
    [string] $OutputFile="$($Database)" + "-Web-Mappings-" + (Get-Date -Format yyyy-MM-dd) + ".sql",

    # Database user name
    [Parameter(Mandatory=$false,
               Position=3)]
    [Alias("U")]
    [string] $DatabaseUser,

    # Database password
    [Parameter(Mandatory=$false,
               Position=4)]
    [Alias("P")]
    [string] $DatabasePassword
)

Add-Type -AssemblyName System.Data

Function Convert-QueryDataToSQL
{
    [CmdletBinding()]
    
    [OutputType([String])]
    Param
    (
        # SQL Server instance name, default to local server
        [Parameter(Mandatory=$true, 
                   Position=0)]
        [Alias("S")] 
        [string]$ServerInstance=$env:ComputerName,

        # Database Name
        [Parameter(Mandatory=$false,
                   Position=1)]
        [AllowEmptyCollection()]
        [string] $Database='Titanium',
        
        # Database user name
        [Parameter(Mandatory=$false,
                   Position=2)]
        [Alias("U")]
        [string] $DatabaseUser,

        # Database password
        [Parameter(Mandatory=$false,
                   Position=3)]
        [Alias("P")]
        [string] $DatabasePassword,

        # Query
        [Parameter(Mandatory=$true,
                   Position=4)]
        [String] $Query
    )
    
    [string[]]$columns = '';
    [string] $insert_columns = '';
    [string] $insert_values = '';
    [string] $tablename = '';
	[string] $sqlcmd = '';
    [string] $ret_value = '';
    [string] $merge_columns = '';
    [string] $merge_values = '';
    [string] $configid = '';

    try {
			$svr = New-Object System.Data.SqlClient.SqlConnection;
            if($DatabaseUser) 
            {   
                $svr.ConnectionString = "Server = $ServerInstance; Database = $Database; User Id = $DatabaseUser; Password = $DatabasePassword";
            } else {
                $svr.ConnectionString = "Server = $ServerInstance; Database = $Database; Integrated Security = True";
            }
            if ($svr -eq $null) {
                Write-Error "Could not connect to $ServerInstance";
                return -1
            }
    		$SqlQuery = New-Object System.Data.SqlClient.SqlCommand;
			$SqlQuery.Connection = $svr;
			$SqlQuery.CommandText = $Query;
			$SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter;
			$SqlAdapter.SelectCommand = $SqlQuery;
			
			$result = New-Object System.Data.DataSet;
			$discard = $SqlAdapter.Fill($result); #prevent SQL Adapter writing return value to output file
            if ($result.Tables.count -gt 0) # we now start to generate the strings
            {
                foreach ($t in $result.tables) #loop through each DataTable
                {
                      Foreach ($r in $t.Rows) #loop through each DataRow
                      {
                          $tablename = $query.Split()[-1];#gets the table name
                          $insert_columns = " ("; 
                          $insert_values = " (";
                          $merge_values = " (";
                          Foreach ($c in $t.Columns) #loop through each DataColumn
                          {
                            if ($r.item($c) -is 'DBNULL')
                            { $itm = 'NULL';}
                            else
                            { 
                                if ($c.datatype.name -eq 'DateTime')
                                {$itm = $r.item($c).tostring("yyyy-MM-dd hh:mm:ss.fff");}
                                else
                                {$itm = $r.item($c).tostring().trim();}
                            
                            }
							
                            $itm = $itm.replace("'", "''");
							
                            if ($itm -eq 'Null') {
								$insert_columns += "$c,";
                                $insert_values += "NULL,";
                                $merge_values += "NULL as " + "$c," ;
							} else {
                                if ($c.ColumnName -eq "Id") {$configid = $c}; #get the config id for this row
                                switch ($c.DataType.name) 
                                {
                                    {('Guid', 'String', 'DateTime') -contains $_} {
										$insert_columns += "$c,"
                                        $insert_values += "'" + $itm + "',"; 
                                        $merge_values += "'" + $itm + "' as " + "$c,";
										break;
									} 
                                    {('int32', 'int64', 'Decimal', 'double') -contains $_} {
										$insert_columns += "$c,"
                                        $insert_values += $itm + ","; 
                                        $merge_values += $itm + " as " + "$c,"
										break;
									} 
                                    {('boolean') -contains $_} {
										if ($r.item($c)) {
											$insert_columns += "$c,";
                                            $insert_values += '1,';
                                            $merge_values += "1 as $c,";
										} else {
											$insert_columns += "$c,";
                                            $insert_values += '0,';
                                            $merge_values += "0 as $c,"
										}; 
										break;
									} 
                                    {$_ -contains ('byte[]')} { 
                                        if ($c.ColumnName -eq "Timestamp") { break; } # the TimeStamp column auto-updates
										$insert_columns += "$c,"
                                        $insert_values += '0x'+[System.BitConverter]::ToString($r.item($c)).replace('-', '')+",";
                                        $merge_values += '0x'+[System.BitConverter]::ToString($r.item($c)).replace('-', '')+" as " + "$c,";
										break; 
									}
                                   # {$_ -contains ('DateTime')} {$insert_columns +="$c="+"'" + $itm + "',"; break;} 
                                    

                                    default {
                                        $insert_columns += "$c,"# ="+"'" + $r.item($c) + "',"; 
                                        $insert_values += "'" + $r.item($c) + "',";
                                        $merge_values += "'" + $r.item($c) + "' as " + "$c,";
										break;
									} 

                                }#switch
                            }#else, i.e. $itm ne  'Null'

                          }#column loop
						  
						#remove trailing comma and replace with close bracket and line breaks
						$insert_columns = $insert_columns.substring(0,$insert_columns.length - 1) + ")";
                        $insert_values = $insert_values.substring(0,$insert_values.length - 1) + ")`r`n";

                        $separator = [string[]]@("Deleted,"); #this mess is so I can split the string after the "deleted" column
                        $merge_columns = "(" + $insert_columns.split($separator, [System.StringSplitOptions]::RemoveEmptyEntries)[1];

                        $mergecol_array = $merge_columns.substring(1); #remove the brackets and split up by comma
                        $mergecol_array = $mergecol_array.substring(0,$mergecol_array.length - 1);
                        $mergecol_array = $mergecol_array.split(",")
                        $sqlcmd = "MERGE " + $tablename + " AS [target] USING SELECT" + $merge_values + " AS [source] on [target].Id = [source].Id when matched then update set ";
                        #generate the merge part of the sql merge
                        foreach ($col in $mergecol_array) {
                            $mrg = "[target].[$col] = [source].[$col],";
                            $sqlcmd += $mrg;
                        }
                        $sqlcmd = $sqlcmd.substring(0,$sqlcmd.length - 1); #trailing comma
                        $sqlcmd += " WHEN NOT MATCHED THEN INSERT " + $insert_columns + " " + $insert_values;

                        $ret_value += $sqlcmd;

                      } #row loop
                    
                }#table loop

            }# $result.Tables.count -gt 0
            else
            {
                Write-Output "No data returned";
                return;
            }
            Write-Output $ret_value;
            return;
    }
    catch
    {
        $ex = $_.Exception
        Write-Error "$ex.Message"
    }
}#Convert-QueryDataToSQL


$top = "-- Web configuration scripts from " + $Database + " on " + (Get-Date -Format yyyy-MM-dd) + "`r`n"

$top | Out-File -FilePath $OutputFile -Force
if($DatabaseUser) { #SQL authentication
    Convert-QueryDataToSQL -ServerInstance $ServerInstance -Database $Database -DatabaseUser $DatabaseUser -DatabasePassword $DatabasePassword -Query "select * from Configuration" | out-file -Append -FilePath $OutputFile 
    Convert-QueryDataToSQL -ServerInstance $ServerInstance -Database $Database -DatabaseUser $DatabaseUser -DatabasePassword $DatabasePassword -Query "select * from ConfigurationAttributes" | out-file -Append -FilePath $OutputFile 
    Convert-QueryDataToSQL -ServerInstance $ServerInstance -Database $Database -DatabaseUser $DatabaseUser -DatabasePassword $DatabasePassword -Query "select * from ConfigurationData" | out-file -Append -FilePath $OutputFile 
    Convert-QueryDataToSQL -ServerInstance $ServerInstance -Database $Database -DatabaseUser $DatabaseUser -DatabasePassword $DatabasePassword -Query "select * from ConfigurationGroup" | out-file -Append -FilePath $OutputFile 
    Convert-QueryDataToSQL -ServerInstance $ServerInstance -Database $Database -DatabaseUser $DatabaseUser -DatabasePassword $DatabasePassword -Query "select * from ConfigurationGroupMembership" | out-file -Append -FilePath $OutputFile 
} else { #Integrated authentication
    Convert-QueryDataToSQL -ServerInstance $ServerInstance -Database $Database -Query "select * from Configuration" | out-file -Append -FilePath $OutputFile 
    Convert-QueryDataToSQL -ServerInstance $ServerInstance -Database $Database -Query "select * from ConfigurationAttributes" | out-file -Append -FilePath $OutputFile 
    Convert-QueryDataToSQL -ServerInstance $ServerInstance -Database $Database -Query "select * from ConfigurationData" | out-file -Append -FilePath $OutputFile 
    Convert-QueryDataToSQL -ServerInstance $ServerInstance -Database $Database -Query "select * from ConfigurationGroup" | out-file -Append -FilePath $OutputFile 
    Convert-QueryDataToSQL -ServerInstance $ServerInstance -Database $Database -Query "select * from ConfigurationGroupMembership" | out-file -Append -FilePath $OutputFile 
}
Write-Output "Done!"
