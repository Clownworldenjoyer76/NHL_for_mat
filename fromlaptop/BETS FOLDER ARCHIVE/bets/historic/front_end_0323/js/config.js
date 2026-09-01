window.REPO_CONFIG = {

  leagues: ["NHL","NBA","NCAAB"],

  NHL:{
    sport:"hockey",
    league:"NHL",
    displayName:"NHL",
    isHockey:true,
    selectFiles:(date)=>[`win/hockey/04_select/${date}_NHL.csv`],
    predFile:(date)=>`win/hockey/00_intake/predictions/hockey_${date}.csv`,
    bookFile:(date)=>`win/hockey/00_intake/sportsbook/hockey_${date}.csv`
  },

  NBA:{
    sport:"basketball",
    league:"NBA",
    displayName:"NBA",
    isHockey:false,

    // 🔑 forces correct header + grouping
    headerKey:"NBA",
    market:"NBA",

    selectFiles:(date)=>[
      `win/basketball/04_select/daily_slate/nba_selected.csv`
    ],
    predFile:(date)=>`win/basketball/00_intake/predictions/basketball_NBA_${date}.csv`,
    bookFile:(date)=>`win/basketball/00_intake/sportsbook/basketball_NBA_${date}.csv`
  },

  NCAAB:{
    sport:"basketball",
    league:"NCAAB",
    displayName:"NCAAB",
    isHockey:false,

    // 🔑 forces correct header + grouping
    headerKey:"NCAAB",
    market:"NCAAB",

    selectFiles:(date)=>[
      `win/basketball/04_select/daily_slate/ncaab_selected.csv`
    ],
    predFile:(date)=>`win/basketball/00_intake/predictions/basketball_NCAAB_${date}.csv`,
    bookFile:(date)=>`win/basketball/00_intake/sportsbook/basketball_NCAAB_${date}.csv`
  }

};
