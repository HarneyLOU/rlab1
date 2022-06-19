# 1. Wczytaj plik autoSmall.csv i wypisz pierwsze 5 wierszy



data <- read.csv("autaSmall.csv", encoding = "UTF-8")
head(data, 5)
nrow(data)



# 2. Pobierz dane pogodowe z REST API
install.packages("httr")
install.packages("jsonlite")

library(httr)
library(jsonlite)

endpoint <- "https://api.openweathermap.org/data/2.5/weather?q=Warszawa&appid=1765994b51ed366c506d5dc0d0b07b77"

getWeather <- GET(endpoint)
weatherText <- content(getWeather,"text")
View(weatherText)
weatherJSON<-fromJSON(weatherText)
wdf<- as.data.frame(weatherJSON)
View(wdf)

# 3. Napisz funkcję zapisującą porcjami danych plik csv do tabeli w SQLite
#Utworzenie bazy na podstawie pliku auta2.csv - 3.2GB

install.packages("DBI")
install.packages("RSQLite")

library(DBI)
library(RSQLite)

con <- dbConnect(SQLite(), "auta2.sqlite")

readToBase<-function(filepath,con,tablename,size=100, sep=",",header=TRUE,delete=TRUE, encoding="UTF-8"){
  
  fileCon <- file(description=filepath, open = "r", encoding = encoding)
  
  df1 <- read.table(fileCon, header = TRUE, sep=sep, fill=TRUE,
                    fileEncoding = encoding, nrows = size)
  if( nrow(df1)==0)
    return(0)
  myColNames <- names(df1)
  dbWriteTable(con, tablename, df1, append=!delete, overwrite=delete)

  repeat{
    if(nrow(df1)==0){
      close(fileCon)
      dbDisconnect(con)
      break;
    }
    df1 <- read.table(fileCon, col.names = myColNames, sep=sep,
                      fileEncoding = encoding, nrows = size)
    dbWriteTable(con, tablename, df1, append=TRUE, overwrite=FALSE)
  }
}

readToBase("auta2.csv", con, "auta2", 1000)

#4.Napisz funkcję znajdującą tydzień obserwacji z największą średnią ceną ofert korzystając z zapytania SQL.

dbListTables(con)

res <- dbSendQuery(con, "
                    SELECT tydzien, AVG(cena) avg_cena 
                    FROM auta2 
                    GROUP BY tydzien 
                    ORDER BY avg_cena DESC
                    LIMIT 1")
View(dbFetch(res))

#5. Podobnie jak w poprzednim zadaniu napisz funkcję znajdującą tydzień obserwacji z największą średnią ceną ofert  tym razem wykorzystując REST api.
#mean(as.data.frame(fromJSON("http://54.37.136.190:8000/week?t=200"))$cena)

findMaxMeanWeek <-function(){
  
  numberOfWeeks <- fromJSON("http://54.37.136.190:8000/nweek")
  numberOfWeeks[1] 
  
  i <- 1
  maxMean <- 0
  maxWeek <- 1
  
  while (i <= numberOfWeeks[1]) {
    endpointCar <- paste("http://54.37.136.190:8000/week?t=", i, sep="")
    autaJSON<-fromJSON(endpointCar)
    autaWeek<- as.data.frame(autaJSON)
    if (mean(autaWeek$cena) > maxMean) {
      maxMean <- mean(autaWeek$cena)
      maxWeek = i
    }
    print(mean(autaWeek$cena))
    print(i)
    i = i+1
  }
  print(maxWeek)
  print(maxMean)
}
findMaxMeanWeek()