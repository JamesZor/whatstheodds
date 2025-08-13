import logging
from typing import Dict, List, Optional

logger = logging.getLogger('bets.data_processing.team_names_mapping')



TEAM_MAPPING_RESULTS = {
    'Arsenal':'Arsenal',
    'Aston Villa': 'Aston_Villa',
    'Bournemouth':'Bournemouth',
    'Brentford':'Brentford',
    'Brighton':'Brighton',
    'Burnley':'Burnley',
    'Chelsea':'Chelsea',
    'Crystal Palace':'Crystal_Palace', 
    'Everton':'Everton',
    'Fulham':'Fulham',
    'Liverpool':'Liverpool',
    'Luton':'Luton',
    'Man City':'Manchester_City',
    'Man United':'Manchester_United',
    'Newcastle':'Newcastle',
    "Nott'm Forest":'Nottingham_Forest',
    'Sheffield United':'Sheffield_United',
    'Tottenham':'Tottenham',
    'West Ham':'West_Ham',
    'Wolves':'Wolves'
}

TEAM_MAPPING_BETFAIR = {
    'Arsenal':'Arsenal',
    'Aston Villa': 'Aston_Villa',
    'Bournemouth':'Bournemouth',
    'Brentford':'Brentford',
    'Brighton':'Brighton',
    'Burnley':'Burnley',
    'Chelsea':'Chelsea',
    'Crystal Palace':'Crystal_Palace', 
    'Everton':'Everton',
    'Fulham':'Fulham',
    'Liverpool':'Liverpool',
    'Luton':'Luton',
    'Man City':'Manchester_City',
    'Man Utd':'Manchester_United',
    'Newcastle':'Newcastle',
    'Nottm Forest':'Nottingham_Forest',
    'Sheff Utd':'Sheffield_United',
    'Tottenham':'Tottenham',
    'West Ham':'West_Ham',
    'Wolves':'Wolves'
}


NEW_TEAM_MAPPING = {
    # English Professional Leagues (EFL)
    'AFC Wimbledon': 'AFC_Wimbledon',
    'Accrington': 'Accrington_Stanley',
    'Barnsley': 'Barnsley',
    'Blackpool': 'Blackpool',
    'Bolton': 'Bolton_Wanderers',
    'Bristol City': 'Bristol_City',
    'Bradford': 'Bradford_City',
    'Cambridge Utd': 'Cambridge_United',
    'Carlisle': 'Carlisle_United',
    'Charlton': 'Charlton_Athletic',
    'Cheltenham': 'Cheltenham_Town',
    'Colchester': 'Colchester_United',
    'Coventry': 'Coventry_City',
    'Crawley Town': 'Crawley_Town',
    'Crewe': 'Crewe_Alexandra',
    'Derby': 'Derby_County',
    'Doncaster': 'Doncaster_Rovers',
    'Exeter': 'Exeter_City',
    'Fleetwood Town': 'Fleetwood_Town',
    'Forest Green': 'Forest_Green_Rovers',
    'Gillingham': 'Gillingham',
    'Grimsby': 'Grimsby_Town',
    'Hartlepool': 'Hartlepool_United',
    'Huddersfield': 'Huddersfield_Town',
    'Hull': 'Hull_City',
    'Ipswich': 'Ipswich_Town',
    'Leeds': 'Leeds_United',
    'Leicester': 'Leicester_City',
    'Leyton Orient': 'Leyton_Orient',
    'Lincoln': 'Lincoln_City',
    'MK Dons': 'Milton_Keynes_Dons',
    'Mansfield': 'Mansfield_Town',
    'Middlesbrough': 'Middlesbrough',
    'Millwall': 'Millwall',
    'Morecambe': 'Morecambe',
    'Newport County': 'Newport_County',
    'Norwich': 'Norwich_City',
    'Notts Co': 'Notts_County',
    'Oldham': 'Oldham_Athletic',
    'Oxford Utd': 'Oxford_United',
    'Peterborough': 'Peterborough_United',
    'Plymouth': 'Plymouth_Argyle',
    'Port Vale': 'Port_Vale',
    'Portsmouth': 'Portsmouth',
    'Preston': 'Preston_North_End',
    'QPR': 'Queens_Park_Rangers',
    'Reading': 'Reading',
    'Rochdale': 'Rochdale',
    'Rotherham': 'Rotherham_United',
    'Salford City': 'Salford_City',
    'Scunthorpe': 'Scunthorpe_United',
    'Sheff Wed': 'Sheffield_Wednesday',
    'Shrewsbury': 'Shrewsbury_Town',
    'Southampton': 'Southampton',
    'Southend': 'Southend_United',
    'Stevenage': 'Stevenage',
    'Stoke': 'Stoke_City',
    'Sunderland': 'Sunderland',
    'Sutton Utd': 'Sutton_United',
    'Swansea': 'Swansea_City',
    'Swindon': 'Swindon_Town',
    'Tranmere': 'Tranmere_Rovers',
    'Walsall': 'Walsall',
    'Watford': 'Watford',
    'West Brom': 'West_Bromwich_Albion',
    'Wigan': 'Wigan_Athletic',
    'Wrexham': 'Wrexham',
    'Wycombe': 'Wycombe_Wanderers',
    'Yeovil': 'Yeovil_Town',
    'York City': 'York_City',

    # Scottish Professional Leagues
    'Aberdeen': 'Aberdeen',
    'Celtic': 'Celtic',
    'Dundee': 'Dundee',
    'Dundee Utd': 'Dundee_United',
    'Dunfermline': 'Dunfermline_Athletic',
    'Falkirk': 'Falkirk',
    'Hamilton': 'Hamilton_Academical',
    'Hearts': 'Heart_of_Midlothian',
    'Hibernian': 'Hibernian',
    'Inverness CT': 'Inverness_Caledonian_Thistle',
    'Kilmarnock': 'Kilmarnock',
    'Livingston': 'Livingston',
    'Motherwell': 'Motherwell',
    'Partick': 'Partick_Thistle',
    'Queen of South': 'Queen_of_the_South',
    'Rangers': 'Rangers',
    'Ross Co': 'Ross_County',
    'St Johnstone': 'St_Johnstone',
    'St Mirren': 'St_Mirren',
    
    # National League Teams
    'AFC Fylde': 'AFC_Fylde',
    'Aldershot': 'Aldershot_Town',
    'Barnet': 'Barnet',
    'Barrow': 'Barrow',
    'Boreham Wood': 'Boreham_Wood',
    'Bromley': 'Bromley',
    'Chesterfield': 'Chesterfield',
    'Dag and Red': 'Dagenham_and_Redbridge',
    'Dover': 'Dover_Athletic',
    'Eastleigh': 'Eastleigh',
    'FC Halifax Town': 'Halifax_Town',
    'Gateshead': 'Gateshead',
    'Maidenhead': 'Maidenhead_United',
    'Solihull Moors': 'Solihull_Moors',
    'Southport': 'Southport',
    'Torquay': 'Torquay_United',
    'Woking': 'Woking',
}

MERGED_TEAM_NAMES = {**TEAM_MAPPING_RESULTS, **TEAM_MAPPING_BETFAIR, **NEW_TEAM_MAPPING}


# Season teams data
SEASON_TEAMS = {
    '2023_2024': {
        'pl_23_24': [
            'Arsenal', 'Aston Villa', 'Bournemouth', 'Brentford', 
            'Brighton', 'Burnley', 'Chelsea', 'Crystal Palace', 
            'Everton', 'Fulham', 'Liverpool', 'Luton',
            'Man City', 'Manchester United', 'Newcastle',
            'Nottingham Forest', 'Sheff Utd', 'Tottenham',
            'West Ham', 'Wolves'
        ],
        'ch_23_24': [
            'Birmingham', 'Blackburn', 'Bristol City', 'Cardiff',
            'Coventry', 'Huddersfield', 'Hull', 'Ipswich',
            'Leeds', 'Leicester', 'Middlesbrough', 'Millwall',
            'Norwich', 'Plymouth', 'Preston', 'QPR',
            'Rotherham', 'Sheffield Wednesday', 'Southampton', 
            'Stoke', 'Sunderland', 'Swansea', 'Watford', 'West Brom'
        ]
    }
}

def get_teams_for_season(season: str) -> Dict[str, List[str]]:
    """
    Get standardized team names for a given season identifier.
    
    Args:
        season: Season identifier (e.g., 'pl_23_24')
        
    Returns:
        Dictionary of league teams with standardized names
    """
    # Extract year from season (e.g., '23_24' from 'pl_23_24')
    year = f"20{season.split('_')[1]}_20{season.split('_')[2]}"
    logger.debug(f"year : {year} ")  

    if year not in SEASON_TEAMS:
        logger.error(f"No team data found for season {year}")
        return {}
        
    # Get teams for all leagues in this season
    season_short = season.split('_')[0]  # e.g., 'pl'
    return {
        league: [standardize_team_name(team) for team in teams]
        for league, teams in SEASON_TEAMS[year].items()
        if league.startswith(season_short)
    }

def standardize_team_name(team_name: str) -> str:
    """
    Convert team name to standard format using mapping.
    
    Args:
        team_name: Team name to standardize
        
    Returns:
        Standardized team name
    """
    if not team_name:
        logger.warning(f"Received empty team name")
        return team_name

    standard_name = MERGED_TEAM_NAMES.get(team_name)
    
    if standard_name is None:
        logger.warning(f"No mapping found for team: '{team_name}'")
        return team_name
    else:
        return standard_name

