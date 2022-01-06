## CASE STUDY: Airline on-time performance

# librerie
library(sparklyr)
library(tidyverse)
library(trelliscopejs)
library(ggmap)
library(usmap)
library(stringr)

# DATI
# Connessione a Spark
sc <- spark_connect(master = "local")
# Disconessione con Spark
spark_disconnect(sc) 

# Spark web
spark_web(sc)

# Importazione dati 
flights_tbl <- spark_read_csv( path = "C:/Users/Roberto/Desktop/Statistica BD/Statistica per i Big Data/Metodi Statistici/r/csv flights/*.csv", sc = sc)

# Dimensione dataset
sdf_dim(flights_tbl)

# Scrittura/Lettura file parquet
spark_write_parquet(Flights_tbl, path = "C:/Users/Roberto/Desktop/Statistica per i Big Data/Metodi Statistici per i Big Data/Flights", mode = "append")

flights_tbl <- spark_read_parquet(sc, path = "C:/Users/Roberto/Desktop/Statistica BD/Statistica per i Big Data/2_Metodi Statistici/r/Flights_pqt")

# PRE-PROCESSING
## Subset dei dati
flights_tbl <- flights_tbl %>%
  select(Year:DayOfWeek,UniqueCarrier,ArrDelay,DepDelay,Origin,Dest,Distance,WeatherDelay,CarrierDelay,NASDelay,SecurityDelay,LateAircraftDelay)

# Sample 5%
flights_spl5 <- sdf_sample(flights_tbl,
                           fraction=0.05,
                           replacement=FALSE,
                           seed=NULL) 

# Left Join con database degli aeroporti
airports <- read.csv("./csv flights/altro/airports.csv")

flights_spl <- flights_spl %>%
  left_join(airports,by = c("Origin"="iata")) %>%
  select(-c(country,lat,long))

# Query
minDelays_dataset <- flights_tbl %>%
  group_by(Month) %>%
  select(Month,DayOfWeek,ArrDelay,DepDelay) %>%
  summarise(avgArrDel = mean(ArrDelay), avgDepDel = mean(DepDelay)) %>%
  arrange(avgArrDel) %>%
  collect()

minDelaysMD_dataset <- minDelays_dataset %>%
  gather("type","value",c(avgArrDel,avgDepDel))

# PLOTS
# Ritardi dei voli
minDM_dataplot <- ggplot(minDelaysMD_dataset,aes(as.factor(Month),value)) +
  geom_bar(aes(fill=type),stat = "identity", position = "dodge",color="white") +
  labs(fill="Type of Delay",title = "Average Delays per Month", x = "Months", y = "Avg Delays") +
  theme_bw() +
  scale_fill_discrete(labels=c("Average Arrival D.","Average Departure D."))

minDow_dataplot <- ggplot(minDelaysDW_dataset,aes(as.factor(DayOfWeek),value)) +
  geom_bar(aes(fill=type),stat = "identity", position = "dodge",color="white") +
  labs(fill="Type of Delay",title = "Average Delays per Days of Week", x = "Day of Week", y = "Avg Delays") +
  theme_bw() +
  scale_fill_discrete(labels=c("Average Arrival D.","Average Departure D."))

# Compagnie aeree
carrFlyDel_dataplot <- ggplot(carrFly_dataset,aes(reorder(UniqueCarrier,-avgArrDel),avgArrDel)) +
  geom_bar(aes(fill=avgDepDel),stat='identity',width=.5) +
  coord_flip() +
  labs(title = "Average Arrival Delays Carriers", y = "Delay", x= "Carrier")


# Left Join con carriers
carriers <- read.csv("./csv flights/altro/carriers.csv")

carrFly <- flights_spl5 %>%
  group_by(UniqueCarrier) %>%
  select(UniqueCarrier,ArrDelay,DepDelay,CarrierDelay) %>%
  summarise(avgArrDel = mean(ArrDelay), avgDepDel = mean(DepDelay), avgCarrDel= mean(CarrierDelay, na.rm=T), n=n()) %>%
  collect()

# left join
carrFly <- carrFly %>%
  left_join(carriers, by = c("UniqueCarrier"="Code"))

# con media pesata
wcarrFly <- carrFly2 %>%
  slice_min(n=5, wmean) %>%
  select(UniqueCarrier,Description,wmean) 

# frequenze relative
freqrel_dataplot<- ggplot(carrFlyWmean)+
  geom_point(aes(reorder(UniqueCarrier,pObs),pObs)) +
  theme_bw() +
  labs(title = "Relative Frequencies Carriers", x = "Carriers", y= "Rel.Freq.")

# BEST TO FLY
wcarrFlyB_data <- carrFlyWmean %>%
  slice_min(n=5, wmean) %>%
  select(UniqueCarrier,Description) 

# WORST TO FLY
wcarrFlyW_data <- carrFlyWmean %>%
  slice_max(n=5, wmean) %>%
  select(UniqueCarrier,Description)

## Carriers flight cancelled
# A = carrier, B = weather, C = NAS(national airpsace sec.), D = security
carrCanc <- flights_spl5 %>%
  group_by(UniqueCarrier,CancellationCode) %>%
  filter(!is.na(CancellationCode)) %>%
  select(UniqueCarrier,CancellationCode) %>%
  summarise(n=n()) %>%
  collect()

carrCanc2 <- carrCanc %>%
  filter(UniqueCarrier == "XE") %>%
  filter(CancellationCode != "NA")

carrCanc_dataplot <- ggplot(carrCanc2_data,aes(reorder(UniqueCarrier,n),n)) +
  geom_bar(aes(fill= CancellationCode),stat = "identity") +
  coord_flip()+
  labs(title = "Cancellation Reasons", x="N",y="Carrier")+
  scale_fill_discrete(labels=c("A=Carrier","B=Weather","C=NAS","D=Security"))

# Aeroporti
wairpFlyBE_data <- wairpFly_data %>%
  slice_min(n=5, wmean) %>%
  select(Origin,name) 
wairpFlyBE_data

#worst
wairpFlyWO_data <- wairpFly_data %>%
  slice_max(n=5, wmean) %>%
  select(Origin,name) 
wairpFlyWO_data

# plot map
usa <- map_data("state",region = ".")
wairpFlyUSA <- filter(wairpFly, lon >-130)
nomi2 <- word(wairpFlyUSA$name, 1,2, sep=" ")

plotmap <-ggplot() +
  geom_polygon(usa,mapping = aes(x=long, y = lat, group = group),color="black",fill="white")+ 
  geom_point(data=wairpFlyUSA_data, aes(x=lon, y=lat,color = avgArrDel),alpha=.9,size=3) +
  geom_text(data=wairpFlyUSA_data, mapping = aes(x=lon,y=lat,label = ifelse(avgArrDel>20 | avgArrDel < 0,nomi2,"")),size=3) +
  coord_fixed(1.3) +
  theme_bw() +
  labs(title = "Airports Delays") 

# SEASONAL FACTORS
seasFact <- flights_spl5 %>%
  group_by(Year,Month) %>%
  summarise(avgArrDel=mean(ArrDelay),avgDepDel=mean(DepDelay),avgWDel=mean(WeatherDelay,na.rm = T)) %>%
  collect()

seasFact_dataplot <- ggplot(seasFact_data, aes(Month,avgArrDel))+
  geom_bar(stat = "identity",fill="steelblue") +
  geom_line(aes(y=avgDepDel),color="red") +
  facet_wrap(~Year) +
  scale_x_discrete(limits = c(1:12)) +
  geom_vline(xintercept = c(4.5,8.5),linetype="dotted",size=.9) +
  labs(title="Average Delays per Month each Year", y="Average Delays", x="Months")


# FLIGHTS VOLUME
volume <- flights_spl5 %>%
  group_by(Year,Month) %>%
  summarise(n=n()) %>%
  collect()

volume_dataplot <- ggplot(volume_data, aes(Month,n))+
  geom_line(color="red",size=1) +
  facet_wrap(~Year) +
  scale_x_discrete(limits = c(1:12)) +
  labs(title="Flights Volume per Year", y="N", x="Months") +
  theme_bw()


volume911 <- flights_spl5 %>%
  group_by(Year) %>%
  filter(Year >= "2001" & Year <="2004") %>%
  select(Year,Month,Origin,Dest,Cancelled) %>%
  summarise(n=n(),ncanc=sum(Cancelled)) %>%
  collect()

volume911_M <- flights_spl5 %>%
  group_by(Year,Month) %>%
  filter(Year >= "2000" & Year <="2004") %>%
  select(Year,Month,Origin,Dest,Cancelled) %>%
  summarise(n=n(),ncanc=sum(Cancelled)) %>%
  collect()

sdf_dim(volume911)
volume911_M %>%
  arrange(Year,Month)
volume911_M$cols <- ifelse(volume911_M$Year == "2000","2000",
                           ifelse(volume911_M$Year == "2001","2001",
                                  ifelse(volume911_M$Year == "2002","2002",
                                         ifelse(volume911_M$Year == "2003","2003","2004"))))

voli00_04 <- ggplot(data=volume911_M, aes(x=Month,y=n,color=cols)) +
  geom_line() +
  geom_point() +
  labs(title = "Volume Flights 2000-2004", x="Month",y="N-Flights",col="Anni") +
  theme_bw() +
  scale_x_discrete(limits = c(1:12))

canc00_04data <- ggplot(data=volume911_Mdata, aes(x=Month,y=ncanc,color=cols)) +
  geom_line() +
  geom_point() +
  labs(title = "Cancelled Flights 2000-2004", x="Month",y="N-Flights",col="Anni") +
  theme_bw() +
  scale_x_discrete(limits = c(1:12))

# Disconessione con Spark
spark_disconnect(sc) 