setwd("R:/Entwicklung/iQSS und PARSUMO App/json_rankings/Prism")

#clear cache
rm(list=ls())

#packages
library(openxlsx)
library(jsonlite)
library(DatastreamDSWS2R)

#optionen
options(expressions=500000,stringsAsFactors = FALSE)
options(Datastream.Username = "ZPEQ001")
options(Datastream.Password = "FORUM304")

mydsws <- dsws$new()

#Region definieren 
ItemsFileName <- "Items/Items iQSS (neu).txt"
DuplicateFileName <- "Duplikate/Duplikate.txt"
CountryExclusionFileName <- "Duplikate/Countryexclusion.txt"

#Items einlesen und mit Standard Items verbinden
Items_List <- read.table(ItemsFileName, quote="", sep="\t", header=TRUE, comment.char = "")
ItemFormulas <- c("TIME","RIC","NAME","ISIN","GEOGN","ICBIN","ICBSSN","ICBSN","ICBSUN",
                  "P","X(P)~CHF","X(MVC)~USD","X(MVC)~USD","PAD#(X(TRESGCS),C1)")
ItemFormulas <- c(ItemFormulas, as.character(Items_List[,4]),"BDATE","RECNO")

#Liste der Duplikate einlesen
Duplicate_List <- read.table(DuplicateFileName, quote="", sep="\t", header=TRUE, comment.char = "")
CountryExclusion_List <- read.table(CountryExclusionFileName, quote="", sep="\t", header=TRUE, comment.char = "")

#Items der ganzen Welt downloaden
ScreeningDM = matrix()
ScreeningEM = matrix()
Screening = data.frame()

for (it_num in 1:length(ItemFormulas)){
   ItemValuesDM <- mydsws$listRequest(instrument = "LXGDFLD$", datatype = ItemFormulas[it_num],
                                    requestDate = "0D")
   ScreeningDM <- cbind(ScreeningDM,ItemValuesDM[2])
  
  ItemValuesEM <- mydsws$listRequest(instrument = "LXGEFLD$", datatype = ItemFormulas[it_num],
                                     requestDate = "0D")
  ScreeningEM <- cbind(ScreeningEM,ItemValuesEM[2])
}

# ScreeningEM <-mydsws$listRequest(instrument="LXGEFLD$", datatype = ItemFormulas, requestDate = "0D")
ScreeningDM <- ScreeningDM[,-(1)]

ScreeningEM <- ScreeningEM[,-(1)]

Screening <- rbind(ScreeningDM, ScreeningEM)

# datum aussortieren?
Screening$RECNO<-as.numeric(Screening$RECNO)
Screening <- Screening[!is.na(Screening$RECNO),]
Screening <- Screening[,-ncol(Screening)]

minDate<-as.Date(Sys.Date()) - 365
Screening <- Screening[!(Screening$BDATE > minDate),]
Screening <- Screening[,-ncol(Screening)]

# Names
ItemNames <- c("Date","ID","Name","ISIN","GEOGN","ICBIN","ICBSSN","ICBSN","ICBSUN","PRICE",
               "PRICECHF","MVUSD","MVUSDN","ESG",as.character(Items_List[,1]))
names(Screening)[1:ncol(Screening)] <- ItemNames

# Bepunktung erstellen
# mappingNull <- read.table("Items/Mapping Nullgrenze.txt",quote="", sep="\t",header=TRUE) #Datei mit Faktoren, die bei Null invertieren
col.inv <- as.character(Items_List$Inverse) == "YES"

ScreeningBackup<-Screening

  # Länderspezifisches Filtern
  Screening<-ScreeningBackup
  Screening<-Screening[(Screening$GEOGN!="NA"),]
  Screening<-Screening[(Screening$ICBIN!="NA"),]
  #Screening<-Screening[Screening$GEOGN %in% Countries,]
  
  #Suspended , Dead and Delisted Stocks
  Screening<-Screening[!grepl("SUSP - SUSP",Screening$Name,fixed=TRUE),]
  Screening<-Screening[!grepl("DELIST.",Screening$Name,fixed=TRUE),]
  Screening<-Screening[!grepl("DEAD - DEAD",Screening$Name,fixed=TRUE),]
  
  #Duplikate und falsche Länder entfernen
  Screening<-Screening[!Screening$ISIN %in% Duplicate_List$ISIN,]
  Screening<-Screening[!Screening$GEOGN %in% CountryExclusion_List$GEOGN,]
  
  #Rohdaten filtern bzgl. MV und VOLA
  Screening$MVUSD<-as.numeric(Screening$MVUSD)
  Screening$MVUSDN<-as.numeric(Screening$MVUSDN)
  Screening <- Screening[!is.na(Screening$MVUSD),]
  Screening <- Screening[!(Screening$MVUSD < 50),]
  
  Screening$VOLA<-as.numeric(Screening$VOLA)
  Screening<-Screening[!is.na(Screening$VOLA),]
  
  
  #Rohdaten filtern bzgl. verwendeter Items und als numeric da es als character runterlädt:/
  Screening1 <- Screening[,colnames(Screening) %in% Items_List[,1]]
  Screening1 <- Screening1[,as.character(Items_List[,1])]
  Screening1<-as.data.frame(sapply(Screening1, as.numeric))
  
  #Styles erstellen und Itemgewichte innerhalb der Styles berechnen
  #PRitems <- Items_List[Items_List$Group=='Profitability',]
  PRitems <- Items_List[Items_List$Group=='Estimates',]
  GRitems <- Items_List[Items_List$Group=='Growth',]
  MOitems <- Items_List[Items_List$Group=='Momentum',]
  QUitems <- Items_List[Items_List$Group=='Quality',]
  RIitems <- Items_List[Items_List$Group=='Risk',]
  VAitems <- Items_List[Items_List$Group=='Value',]
  
  PRcolnum <- which(colnames(Screening1) %in% PRitems[,1])
  GRcolnum <- which(colnames(Screening1) %in% GRitems[,1])
  MOcolnum <- which(colnames(Screening1) %in% MOitems[,1])
  QUcolnum <- which(colnames(Screening1) %in% QUitems[,1])
  RIcolnum <- which(colnames(Screening1) %in% RIitems[,1])
  VAcolnum <- which(colnames(Screening1) %in% VAitems[,1])
  
  PRWeight <- 1/nrow(PRitems)
  GRWeight <- 1/nrow(GRitems)
  MOWeight <- 1/nrow(MOitems)
  QUWeight <- 1/nrow(QUitems)
  RIWeight <- 1/nrow(RIitems)
  VAWeight <- 1/nrow(VAitems)
  
  # Screening0min <- Screening1[VAcolnum]

  #Funktion 1: Vektor mit Werten umbauen zu Ranking 1-100, mit oG und uG als Grenzen. Darüber 100, darunter 1 konstant
  ranking <- function(a,untereGrenze,obereGrenze){

    a = 1+((obereGrenze-a)/(obereGrenze-untereGrenze)*99)
    a[a>=100] = 100
    a[a<=1] = 1
    a[is.na(a)] <- 50.5

    return(as.data.frame(a))
  }

  rankingINV <- function(a,untereGrenze,obereGrenze){

    a = 1+((a-untereGrenze)/(obereGrenze-untereGrenze)*99)
    a[a>=100] = 100
    a[a<=1] = 1
    a[is.na(a)] <- 50.5

    return(as.data.frame(a))
  }
  
  #Code
  #Ranking der einzelnen items erstellen
  uG1 = 0.0003
  oG1 = 0.9997
  Screening1Roh<-Screening1
  
  for (col in 1:ncol(Screening1)){
    
    itemName<-colnames(Screening1[col])
    itemDetails<-Items_List[Items_List$Items %in% itemName,]
    
    MinValue<-as.numeric(as.character(itemDetails$MinValue))
    MaxValue<-as.numeric(as.character(itemDetails$MaxValue))
    MinRank<-as.numeric(as.character(itemDetails$MinRank))
    MaxRank<-as.numeric(as.character(itemDetails$MaxRank))
    
    if(is.na(MinValue)){
      untereGrenze <- quantile(Screening1[,col], probs = uG1, na.rm = TRUE)
    }else{
      untereGrenze <- max(quantile(Screening1[,col], probs = uG1, na.rm = TRUE),MinValue)
    }
    
    if(is.na(MaxValue)){
      obereGrenze <- quantile(Screening1[,col], probs = oG1, na.rm = TRUE)
    }else{
      obereGrenze <- min(quantile(Screening1[,col], probs = oG1, na.rm = TRUE),MaxValue)
    }
    
    if (col.inv[col]){
      Screening1[,col]<-rankingINV(Screening1[,col],untereGrenze,obereGrenze)
    }else{
      Screening1[,col]<-ranking(Screening1[,col],untereGrenze,obereGrenze)
    }
    
    # apply cutoff rules
    if(itemDetails$MinValue!="n.a."){
      for(i in 1:nrow(Screening1Roh)){
        if(!is.na(Screening1Roh[i,col])){
          if(Screening1Roh[i,col]<=MinValue){
            Screening1[i,col]<-MinRank
          }
        }
      }
    }
    
    if(itemDetails$MaxValue!="n.a."){
      for(i in 1:nrow(Screening1Roh)){
        if(!is.na(Screening1Roh[i,col])){
          if(Screening1Roh[i,col]>=MaxValue){
            Screening1[i,col]<-MaxRank
          }
        }
      }
    }
  }

  PRScreening <- as.data.frame(Screening1[,sort(PRcolnum)]*PRWeight)
  PRtotal <- transform(PRScreening, Total=rowSums(PRScreening))
  PRtotal <- transform(PRtotal, Rank=as.integer(rank(PRtotal$Total, ties.method = "min")))
  PRtotal$Score <- 1+ (PRtotal$Rank-1)/(max(PRtotal$Rank)-1)*9
  
  GRScreening <- as.data.frame(Screening1[,sort(GRcolnum)]*GRWeight)
  GRtotal <- transform(GRScreening, Total=rowSums(GRScreening))
  GRtotal <- transform(GRtotal, Rank=as.integer(rank(GRtotal$Total, ties.method = "min")))
  GRtotal$Score <- 1+ (GRtotal$Rank-1)/(max(GRtotal$Rank)-1)*9
  
  MOScreening <- as.data.frame(Screening1[,sort(MOcolnum)]*MOWeight)
  MOtotal <- transform(MOScreening, Total=rowSums(MOScreening))
  MOtotal <- transform(MOtotal, Rank=as.integer(rank(MOtotal$Total, ties.method = "min")))
  MOtotal$Score <- 1+ (MOtotal$Rank-1)/(max(MOtotal$Rank)-1)*9
  
  QUScreening <- as.data.frame(Screening1[,sort(QUcolnum)]*QUWeight)
  QUtotal <- transform(QUScreening, Total=rowSums(QUScreening))
  QUtotal <- transform(QUtotal, Rank=as.integer(rank(QUtotal$Total, ties.method = "min")))
  QUtotal$Score <- 1+ (QUtotal$Rank-1)/(max(QUtotal$Rank)-1)*9
  
  RIScreening <- as.data.frame(Screening1[,sort(RIcolnum)]*RIWeight)
  RItotal <- transform(RIScreening, Total=rowSums(RIScreening))
  RItotal <- transform(RItotal, Rank=as.integer(rank(RItotal$Total, ties.method = "min")))
  RItotal$Score <- 1+ (RItotal$Rank-1)/(max(RItotal$Rank)-1)*9
  
  VAScreening <- as.data.frame(Screening1[,sort(VAcolnum)]*VAWeight)
  # VAScreening[Screening0min < 0] <- VAWeight * 100 #Das ändert negative Werte im Item als schlecht, d.h. 100 Punkte. Kommt nur bei Value vor
  VAtotal <- transform(VAScreening, Total=rowSums(VAScreening))
  VAtotal <- transform(VAtotal, Rank=as.integer(rank(VAtotal$Total, ties.method = "min")))
  VAtotal$Score <- 1+ (VAtotal$Rank-1)/(max(VAtotal$Rank)-1)*9
  
  # Bepunktung erstellen
  rangliste <- data.frame(Screening[3:8],Screening[12:13],VAtotal$Score,GRtotal$Score,QUtotal$Score,PRtotal$Score,MOtotal$Score,RItotal$Score)
  names(rangliste)[9:14] <- c("Value","Growth","Quality","Estimates","Momentum","Risk")
  
  # additional columns
  additInfo <- data.frame("ESG"=as.numeric(Screening$ESG))
  rangliste <- cbind(rangliste,additInfo)
  rangliste$ESG <- (!is.na(rangliste$ESG)&rangliste$ESG>55)# TODO Check if this works and look where median
  rangliste$MVUSD <- rangliste$MVUSD>2000
  
  # is in some index?
  spTitel <- mydsws$listRequest(instrument = "LS&PCOMP", datatype = "ISIN", requestDate = "0D")
  inSp<-(rangliste$ISIN %in% spTitel$ISIN)
  rangliste$INDEX="NA"
  for(i in 1:nrow(rangliste)){
    if(inSp[i]){
    rangliste$INDEX[i]<-"SP500"
    }
  }
  
  daxTitel <- mydsws$listRequest(instrument = "LDAXINDX", datatype = "ISIN", requestDate = "0D")
  inDax<-(rangliste$ISIN %in% daxTitel$ISIN)
  for(i in 1:nrow(rangliste)){
    if(inDax[i]){
      rangliste$INDEX[i]<-"DAX"
    }
  }
  
  nikTitel <- mydsws$listRequest(instrument = "LJAPDOWA", datatype = "ISIN", requestDate = "0D")
  inNik<-(rangliste$ISIN %in% nikTitel$ISIN)
  for(i in 1:nrow(rangliste)){
    if(inNik[i]){
      rangliste$INDEX[i]<-"NIKKEI"
    }
  }
  
  #JSON konvertierung
  rangliste <- rangliste[,c("Name", "ISIN", "GEOGN", "ICBIN", "ESG", "MVUSD", "MVUSDN", "INDEX", "Value", "Growth", "Quality", "Estimates", "Momentum", "Risk")]
  OutputFileName <- paste("Liste.json",sep="")
  write_json(rangliste, OutputFileName)
  
  OutputFileName <- paste("Liste.xlsx",sep="")
  write.xlsx(rangliste, OutputFileName, colWidths = "auto", zoom = 80,
             sheetName = "Ranking", creator = "Niggi Meyer", title = "PARSUMO QSS Research", overwrite = TRUE)
  
  OutputFileName <- paste("Daten.xlsx",sep="")
  write.xlsx(Screening, OutputFileName, colWidths = "auto", zoom = 80,
             sheetName = "Ranking", creator = "Niggi Meyer", title = "PARSUMO QSS Research", overwrite = TRUE)
  
  
  # write.xlsx(Screening,file="R:/Entwicklung/iQSS und PARSUMO App/json_rankings/Prism/Screening_rohdaten.xlsx",colWidths = "auto", zoom = 80
  #            sheetName = "Ranking", creator = "Niggi Meyer", title = "PARSUMO QSS Research")
  # write.xlsx(Screening1,file="R:/Entwicklung/iQSS und PARSUMO App/json_rankings/Prism/Screening_faktorbepunktung.xlsx",colWidths = "auto", zoom = 80,
  #            sheetName = "Ranking", creator = "Niggi Meyer", title = "PARSUMO QSS Research")
  
  
   # save.image("R:/Entwicklung/QSS/R-code QSS Rangliste/Screening.RData")


