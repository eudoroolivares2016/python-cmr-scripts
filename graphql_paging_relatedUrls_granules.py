"""
Iterates over cmr-graphql (https://github.com/nasa/cmr-graphql/) to
retrieve all of the collection's concept-ids from a given env (SIT, UAT, PROD) which match the string in the regex
a valid token must be passed
as an env variable otherwise only what is publicly available will be able to be fetched
then print the list of these concepts to a local file
"""
import os
import sys
import logging
import requests
import json
import re

granule_cursor = ""

allMatchingGranules = []
# parse the related url
def parseRelatedUrls(collectionsResult):
  for collection in collectionsResult:
    collectionConceptId = collection.get('conceptId')
    granules = collection.get('granules')
    count = granules.get('count')
    granule_cursor = granules.get('cursor')
    # print(f'this is the number of garnules on the collection {count}! On the collection {collectionConceptId} this is the current cursor {granule_cursor}')
    if (granules):
      for granule in granules.get('items'):
        relatedUrls = granule.get('relatedUrls')
        if (relatedUrls):
          for relatedUrl in relatedUrls:
            if relatedUrl:
              urlValue = relatedUrl.get('url')
              if urlValue is None:
                print('💀What is the url', urlValue)
                print('💀What is the conceptId that has this', collection.get('conceptId'))
                print('💀What is the number of related Urls', len(relatedUrls))
              if urlValue and re.search(r"search\..*?\.?earthdata\.nasa\.gov", urlValue):
                matchingGranuleConceptId = granule.get('conceptId')
                print('🚀 ~ file: graphql_paging_relatedUrls.py:26 ~ matchingGranuleConceptId:', matchingGranuleConceptId)
                allMatchingGranules.append(matchingGranuleConceptId)
# Set logging
logging.basicConfig(level=logging.INFO)

token = ""

endpoint = os.getenv('GRAPHQL_URL', 'https://graphql.earthdata.nasa.gov/api')

# Bearer may be needed before the token in your env var
# if you are using an EDL token instead of a launchpad or echo
headers = {"Authorization": f"{token}"}

CURSOR = ""

QUERY = """query ($params: CollectionsInput, , $granulesParams2: GranulesInput) {
  collections(params: $params) {
    cursor
    items {
      conceptId
      granules(params: $granulesParams2) {
        count
        cursor
        items {
        conceptId
        relatedUrls
        }
      }
    }
  }
}"""

VARIABLES = {
    "params": {
      "cursor": CURSOR
    },
    "granulesParams2": {
      "cursor": granules_cursor
    }
}


# pylint: disable=line-too-long
# pylint: disable=logging-fstring-interpolation
response = requests.post(url=endpoint, json={"query": QUERY, "variables":  VARIABLES}, headers=headers, timeout=90)
first_result = response.json()

# Retrieve the first cursor value
if response.status_code == 200:
    cursor = first_result.get('data', {}).get('collections', {}).get('cursor')
    collections = first_result.get('data', {}).get('collections', {}).get('items')
    parseRelatedUrls(collections)

else:
    print("There was a failure in retrieving the first cursor check logs for status code")
    logging.debug(f'Response status code: {response.status_code}')
    sys.exit()

# # # Retrieve subsequent collections
# if response.status_code == 200:
    page_num = 0
    while cursor:
        # Set the cursor value to the global value
        print(f'current page: {page_num}')
        print(f'Search_after cursor values {cursor}')
        newVariables = {
            "params": {
              "cursor": cursor,
            }
        }
        if granule_cursor:
          newVariables = {
            "params": {
              "cursor": cursor,
            },
            "granulesParams2": {
              "cursor": granule_cursor,
            }
            response = requests.post(url=endpoint, json={"query": QUERY, "variables": newVariables}, headers=headers, timeout=90)
        }
        response = requests.post(url=endpoint, json={"query": QUERY, "variables": newVariables}, headers=headers, timeout=90)
        result = response.json()
        collections = result.get('data', {}).get('collections', {}).get('items', [])
        cursor = result.get('data', {}).get('collections', {}).get('cursor')
        logging.debug(f'Response in json: {collections}')
        parseRelatedUrls(collections)
        page_num = page_num + 1
        if response.status_code != 200:
          page_num = page_num - 1
          print('🛑 Failed to get collections', result)
    # Write results to disk
    with open("./granuleLogs/ListOfGranulesWithEDSCRelatedUrls.txt", "w", encoding='UTF-8') as txt_file:
      txt_file.write(f"{allMatchingGranules}\n")
    print(f'Total number of pages {page_num}')
