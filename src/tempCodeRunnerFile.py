
# t1 = time.time()
# L = os.listdir(MATCHES)
# # print(L[2])
# tasks = list(map(int, L))
# #
# # tasks = [3716920,4058253]
# print(f"Got {len(tasks)} Matches")
# with ThreadPoolExecutor(max_workers=80) as executor:
#     with tqdm.tqdm(total=len(tasks)) as pbar:
#         futures = {
#             executor.submit(cricdata, m) for m in tasks
#         }

#         for future in as_completed(futures):
#             # for count, future in enumerate(futures):
#             # print(count, end="\r")
#             try:
#                 future.result()
#             except Exception as e:
# #                 # print(f"Error processing match {future}: {e}")
#                 with open("../logs/matchparsing.log", "a") as l:
#                     l.write(traceback.format_exc())
#             finally:
#                 pbar.update(1)
#     executor.shutdown()
# # Leagues = []
# # serieses = []
# # tours = []
# # venues = []


# df1 = pd.DataFrame(serieses)
# df2 = pd.DataFrame(tours)
# df3 = pd.DataFrame(venues)
# df4 = pd.DataFrame(Leagues)
# df5 = pd.DataFrame(noBbBd)
# # df2 = pd.concat([pd.read_parquet(f"./{dis}/tours.parquet"),df2]).drop_duplicates()
# # df1 = pd.concat([pd.read_parquet(f"./{dis}/serieses.parquet"),df1]).drop_duplicates()
# # df3 = pd.concat([pd.read_parquet(f"./{dis}/venues.parquet"),df3]).drop_duplicates()
# # df4 = pd.concat([pd.read_parquet(f"./{dis}/leagues.parquet"),df4]).drop_duplicates()
# # df5 = pd.concat([pd.read_parquet(f"./{dis}/NoBallbyBall.parquet"),df5]).drop_duplicates()
# df1.to_parquet(f"./{dis}/serieses.parquet")
# df2.to_parquet(f"./{dis}/tours.parquet")
# df3.to_parquet(f"./{dis}/venues.parquet")
# df4.to_parquet(f"./{dis}/leagues.parquet")
# df5.to_parquet(f"./{dis}/NoBallbyBall.parquet")

# t2 = time.time()
# print(f"Time taken: {t2 - t1} seconds")