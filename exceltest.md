ExcelMultipleTablesSelect
```vba
Public Function ExcelToJSONS(rng1 As Range, rng2 As Range, rng3 As Range) As String
    ' Dim json1, json2, json3 As String
    ExcelToJSONS = "{" & ExcelToJSON(rng1) & ExcelToJSON(rng2) & ExcelToJSON(rng3) & "}"
End Function
Public Function ExcelToJSON(rng As Range) As String
    ' Check there must be at least two columns in the Excel file
    If rng.Columns.Count < 2 Then
        ExcelToJSON = CVErr(xlErrNA)
        Exit Function
    End If
    Dim dataLoop, headerLoop As Long
    ' Get the first row of the Excel file as a header
    Dim headerRange As Range: Set headerRange = Range(rng.Rows(1).Address)
    ' Count the number of columns of targeted Excel file
    Dim colCount As Long: colCount = headerRange.Columns.Count
    Dim JSON As String: JSON = "{"
    For dataLoop = 1 To rng.Rows.Count
        ' Skip the first row of the Excel file because it is used as header
        If dataLoop > 1 Then
            ' Start data row
            Dim jsonData As String: jsonData = "{"
            ' Loop through each column and combine with the header
            For headerLoop = 1 To colCount
                jsonData = jsonData & """" & headerRange.Value2(1, headerLoop) & """" & ":"
                jsonData = jsonData & """" & IIf(IsEmpty(rng.Value2(dataLoop, headerLoop)), " ", rng.Value2(dataLoop, headerLoop)) & """"
                jsonData = jsonData & ","
            Next headerLoop
            ' Strip out the comma in last value of each row
            jsonData = Left(jsonData, Len(jsonData) - 1)
            ' End data row
            JSON = JSON & jsonData & "},"
        End If
    Next
    ' Strip out the last comma in last row of the Excel data
    JSON = Left(JSON, Len(JSON) - 1)
    JSON = JSON & "}"
    ExcelToJSON = JSON
End Function
```
