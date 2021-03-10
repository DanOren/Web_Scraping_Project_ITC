import pymysql
import config as cfg
import logging
import sys
import re
import pandas as pd
import numpy as np



logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# Create formatter
formatter = logging.Formatter('%(asctime)s-FILE:%(filename)s-FUNC:%(funcName)s-LINE:%(lineno)d-%(message)s')

# Create a file handler and add it to logger.
file_handler = logging.FileHandler('web_scraper.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
#
# stream_handler = logging.StreamHandler(sys.stdout)
# stream_handler.setLevel(logging.INFO)
# stream_handler.setFormatter(formatter)
# logger.addHandler(stream_handler)

sample_dictionary = {'Platoon_1987': {'Metascore': '92', 'User score': '8.0', 'Title': 'Platoon', 'Release Year': '1987', 'Studio': 'Orion Pictures', 'Director': 'Oliver Stone', 'Genres': ['Drama', 'War'], 'Rating': 'R', 'Runtime': '120 min', 'Summary': 'A young recruit in Vietnam faces a moral crisis when confronted with the horrors of war and the duality of man.'}, 'Tampopo_1987': {'Metascore': '87', 'User score': 'tbd', 'Title': 'Tampopo', 'Release Year': '1987', 'Studio': 'New Yorker Films', 'Director': 'Jûzô Itami', 'Genres': ['Comedy'], 'Rating': 'Not Rated', 'Runtime': '114 min', 'Summary': 'The tale of an enigmatic band of ramen ronin who guide the widow of a noodle shop owner on her quest for the perfect recipe, Tampopo serves up a savory broth of culinary adventure seasoned with offbeat comedy sketches and the erotic exploits of a gastronome gangster. Sweet, sexy, surreal, and mouthwatering, Tampopo remains one of the mostThe tale of an enigmatic band of ramen ronin who guide the widow of a noodle shop owner on her quest for the perfect recipe, Tampopo serves up a savory broth of culinary adventure seasoned with offbeat comedy sketches and the erotic exploits of a gastronome gangster. Sweet, sexy, surreal, and mouthwatering, Tampopo remains one of the most delectable examples of food on film. [Janus Films]… Expand'}, 'Broadcast News_1987': {'Metascore': '84', 'User score': '7.7', 'Title': 'Broadcast News', 'Release Year': '1987', 'Studio': 'Twentieth Century Fox Film Corporation', 'Director': 'James L. Brooks', 'Genres': ['Drama', 'Comedy', 'Romance'], 'Rating': 'R', 'Runtime': '133 min', 'Summary': "Tom Grunick, a rising anchorman, has plenty of on-camera savvy, personality and good looks but little in the way of brains. He may be dumb as a doornail but Tom's got the star presence his network needs. Then there's Aaron Altman, a less than good-looking newsman, who's dedicated, diligent and down-to-earth. Aaron would love to be an anchorTom Grunick, a rising anchorman, has plenty of on-camera savvy, personality and good looks but little in the way of brains. He may be dumb as a doornail but Tom's got the star presence his network needs. Then there's Aaron Altman, a less than good-looking newsman, who's dedicated, diligent and down-to-earth. Aaron would love to be an anchor but he freezes up the moment he's in front of the camera. Spunky, smart news producer Jane Craig is excellent at her job but has difficulty handling the pressure. Can these workaholics mask their personal quirks long enough to jump-start their professional lives? (20h Century Fox)… Expand"}, 'Withnail & I_1987': {'Metascore': '84', 'User score': '6.4', 'Title': 'Withnail & I', 'Release Year': '1987', 'Studio': 'Anchor Bay Entertainment', 'Director': 'Bruce Robinson', 'Genres': ['Drama', 'Comedy'], 'Rating': 'R', 'Runtime': '107 min', 'Summary': "London 1969 - two 'resting' (unemployed and unemployable) actors, Withnail and Marwood, fed up with damp, cold, piles of washing-up, mad drug dealers and psychotic Irishmen, decide to leave their squalid Camden flat for an idyllic holiday in the countryside, courtesy of Withnail's uncle Monty's country cottage. But when they getLondon 1969 - two 'resting' (unemployed and unemployable) actors, Withnail and Marwood, fed up with damp, cold, piles of washing-up, mad drug dealers and psychotic Irishmen, decide to leave their squalid Camden flat for an idyllic holiday in the countryside, courtesy of Withnail's uncle Monty's country cottage. But when they get there, it rains non-stop, there's no food, and their basic survival skills turn out to be somewhat limited. Matters are not helped by the arrival of Uncle Monty, who shows an uncomfortably keen interest in Marwood...… Expand"}, 'Moonstruck_1987': {'Metascore': '83', 'User score': '8.4', 'Title': 'Moonstruck', 'Release Year': '1987', 'Studio': 'Metro-Goldwyn-Mayer (MGM)', 'Director': 'Norman Jewison', 'Genres': ['Drama', 'Comedy', 'Romance'], 'Rating': 'PG', 'Runtime': '102 min', 'Summary': 'In this romantic comedy, Loretta (Cher), a young widow, feels unlucky in love and is content to wed a man she does not love (Aiello)...until she meets and falls hopelessly in love with his younger brother (Cage).'}, 'My Life as a Dog_1987': {'Metascore': '82', 'User score': 'tbd', 'Title': 'My Life as a Dog', 'Release Year': '1987', 'Studio': 'Skouras Pictures', 'Director': 'Lasse Hallstrom', 'Genres': ['Drama', 'Comedy'], 'Rating': 'Unrated', 'Runtime': '101 min', 'Summary': "A boy and his brother don't get along well. In order to let their ill mother rest, they're separated and sent each one with their relatives."}, 'The Untouchables_1987': {'Metascore': '79', 'User score': '8.6', 'Title': 'The Untouchables', 'Release Year': '1987', 'Studio': 'Paramount Pictures', 'Director': 'Brian De Palma', 'Genres': ['Drama', 'History', 'Thriller', 'Crime'], 'Rating': 'R', 'Runtime': '119 min', 'Summary': 'Federal Agent Eliot Ness sets out to stop Al Capone with a small, hand-picked team.'}, 'House of Games_1987': {'Metascore': '78', 'User score': '7.3', 'Title': 'House of Games', 'Release Year': '1987', 'Studio': 'Orion Pictures', 'Director': 'David Mamet', 'Genres': ['Thriller', 'Crime'], 'Rating': 'R', 'Runtime': '102 min', 'Summary': 'A psychiatrist comes to the aid of a compulsive gambler and is led by a smooth-talking grifter into the shadowy but compelling world of stings, scams, and con men.'}, 'The Princess Bride_1987': {'Metascore': '77', 'User score': '8.4', 'Title': 'The Princess Bride', 'Release Year': '1987', 'Studio': 'Twentieth Century Fox Film Corporation', 'Director': 'Rob Reiner', 'Genres': ['Adventure', 'Fantasy', 'Comedy', 'Romance', 'Family'], 'Rating': 'PG', 'Runtime': '98 min', 'Summary': 'A young boy listens to while his grandfather reads him the adventures of Buttercup (Wright), the most beautiful woman in the world, and Westly (Elwes), the man she loves, in the fairy-tale kingdom of Florin. [MGM]'}, 'No Way Out_1987': {'Metascore': '77', 'User score': '8.2', 'Title': 'No Way Out', 'Release Year': '1987', 'Studio': 'Orion Pictures', 'Director': 'Roger Donaldson', 'Genres': ['Action', 'Drama', 'Mystery', 'Thriller', 'Romance', 'Crime'], 'Rating': 'R', 'Runtime': '114 min', 'Summary': 'A coverup and witchhunt ensue after a politician (Gene Hackman) accidentally puts his mistress (Sean Young) in jeopardy.'}, 'The Big Easy_1987': {'Metascore': '77', 'User score': '7.2', 'Title': 'The Big Easy', 'Release Year': '1987', 'Studio': 'Columbia Pictures', 'Director': 'Jim McBride', 'Genres': ['Drama', 'Thriller', 'Romance', 'Crime'], 'Rating': 'R', 'Runtime': '102 min', 'Summary': "Set in New Orleans. Remy McSwain, lieutenant in Homicide finds that he has two problems, the first of a series of gang killings and Ann Osborne, a beautiful attorney from the D.A.'s police corruption task force in his office. He begins a relationship with her as the killings continue only to have charges filed against him for acceptingSet in New Orleans. Remy McSwain, lieutenant in Homicide finds that he has two problems, the first of a series of gang killings and Ann Osborne, a beautiful attorney from the D.A.'s police corruption task force in his office. He begins a relationship with her as the killings continue only to have charges filed against him for accepting bribes as he stumbles on a police corruption Sting. While this is happening, the criminals insist that none of the crime gangs are behind the killings.… Expand"}, 'Full Metal Jacket_1987': {'Metascore': '76', 'User score': '7.4', 'Title': 'Full Metal Jacket', 'Release Year': '1987', 'Studio': 'Warner Bros. Pictures', 'Director': 'Stanley Kubrick', 'Genres': ['Action', 'Drama', 'War'], 'Rating': 'R', 'Runtime': '116 min', 'Summary': 'The story of an 18-year-old marine recruit named Private Joker - from his carnage-and-machismo boot camp to his climactic involvement in the heavy fighting in Hue during the 1968 Tet Offensive. [Warner Bros.]'}, 'Near Dark_1987': {'Metascore': '76', 'User score': '7.8', 'Title': 'Near Dark', 'Release Year': '1987', 'Studio': 'De Laurentiis Entertainment Group (DEG)', 'Director': 'Kathryn Bigelow', 'Genres': ['Action', 'Drama', 'Thriller', 'Horror', 'Romance', 'Crime'], 'Rating': 'R', 'Runtime': '94 min', 'Summary': "A small-town farmer's son reluctantly joins a traveling group of vampires after he is bitten by a beautiful drifter."}, 'The Last Emperor_1987': {'Metascore': '76', 'User score': '8.1', 'Title': 'The Last Emperor', 'Release Year': '1987', 'Studio': 'Columbia Pictures', 'Director': 'Bernardo Bertolucci', 'Genres': ['Biography', 'Drama', 'History'], 'Rating': 'TV-14', 'Runtime': '163 min', 'Summary': "The story of Pu Yi's life from his reign as Emperor to his last days as a peasant worker in the People's Republic."}, 'Tin Men_1987': {'Metascore': '75', 'User score': 'tbd', 'Title': 'Tin Men', 'Release Year': '1987', 'Studio': 'Buena Vista Pictures', 'Director': 'Barry Levinson', 'Genres': ['Drama', 'Comedy'], 'Rating': 'R', 'Runtime': '112 min', 'Summary': 'A minor car accident drives two rival aluminum-siding salesmen to the ridiculous extremes of man versus man in 1963 Baltimore.'}, 'Maurice_1987': {'Metascore': '75', 'User score': '8.6', 'Title': 'Maurice', 'Release Year': '1987', 'Studio': 'Cinecom Pictures', 'Director': 'James Ivory', 'Genres': ['Drama', 'Romance'], 'Rating': 'R', 'Runtime': '140 min', 'Summary': "Cambridge students Clive (Grant) and Maurice (Wilby) fall in love, but Clive soon decides he must take his place in society and marry. Maurice's life is changed when he meets Alec Scudder (Graves), the gamekeeper at Clive's estate."}, 'Radio Days_1987': {'Metascore': '74', 'User score': '7.7', 'Title': 'Radio Days', 'Release Year': '1987', 'Studio': 'Orion Pictures', 'Director': 'Woody Allen', 'Genres': ['Comedy'], 'Rating': 'PG', 'Runtime': '88 min', 'Summary': "A nostalgic look at radio's golden age focusing on one ordinary family and the various performers in the medium."}, 'Hollywood Shuffle_1987': {'Metascore': '74', 'User score': 'tbd', 'Title': 'Hollywood Shuffle', 'Release Year': '1987', 'Studio': 'Samuel Goldwyn Company, The', 'Director': 'Robert Townsend', 'Genres': ['Comedy'], 'Rating': 'R', 'Runtime': '78 min', 'Summary': 'An actor (Robert Townsend) limited to stereotypical roles because of his ethnicity, dreams of making it big as a highly respected performer. As he makes his rounds, the film takes a satiric look at African American actors in Hollywood.'}, 'Matewan_1987': {'Metascore': '73', 'User score': 'tbd', 'Title': 'Matewan', 'Release Year': '1987', 'Studio': 'Cinecom Pictures', 'Director': 'John Sayles', 'Genres': ['Drama', 'History'], 'Rating': 'PG-13', 'Runtime': '135 min', 'Summary': 'A labor union organizer comes to an embattled mining community brutally and violently dominated and harassed by the mining company.'}, 'Roxanne_1987': {'Metascore': '73', 'User score': '9.0', 'Title': 'Roxanne', 'Release Year': '1987', 'Studio': 'Columbia Pictures', 'Director': 'Fred Schepisi', 'Genres': ['Comedy', 'Romance'], 'Rating': 'PG', 'Runtime': '107 min', 'Summary': 'An adaptation of "Cyrano de Bergerac", starring a big-hearted, and big-nosed fire chief (Steve Martin) who falls in love with a beautiful astronomer (Daryl Hannah).'}, "River's Edge_1987": {'Metascore': '73', 'User score': 'tbd', 'Title': "River's Edge", 'Release Year': '1987', 'Studio': 'Island Pictures', 'Director': 'Tim Hunter', 'Genres': ['Drama', 'Crime'], 'Rating': 'R', 'Runtime': '99 min', 'Summary': "A high school slacker commits a shocking act and proceeds to let his friends in on the secret. However, the friends' reaction is almost as ambiguous and perplexing as the crime itself."}, 'Prick Up Your Ears_1987': {'Metascore': '72', 'User score': 'tbd', 'Title': 'Prick Up Your Ears', 'Release Year': '1987', 'Studio': 'Curzon Film Distributors', 'Director': 'Stephen Frears', 'Genres': ['Biography', 'Drama', 'Romance'], 'Rating': 'R', 'Runtime': '105 min', 'Summary': 'This film is the story of the spectacular life and violent death of British playwright Joe Orton. In his teens, Orton is befriended by the older, more reserved Kenneth Halliwell, and while the two begin a relationship, it\'s fairly obvious that it\'s not all about sex. Orton loves the dangers of bath-houses and liaisons in public restrooms;This film is the story of the spectacular life and violent death of British playwright Joe Orton. In his teens, Orton is befriended by the older, more reserved Kenneth Halliwell, and while the two begin a relationship, it\'s fairly obvious that it\'s not all about sex. Orton loves the dangers of bath-houses and liaisons in public restrooms; Halliwell, not as charming or attractive as Orton, doesn\'t fare so well in those environs. While both long to become writers, it is Orton who achieves fame - his plays "Entertaining Mr. Sloane" and "Loot" become huge hits in London of the sixties, and he\'s even commissioned to write a screenplay for the Beatles. But Orton\'s success takes him farther from Halliwell, whose response ended both his life and the life of the up-and-coming playwright.… Expand'}, "Sign 'o' the Times_1987": {'Metascore': '72', 'User score': 'tbd', 'Title': "Sign 'o' the Times", 'Release Year': '1987', 'Studio': 'Cineplex Odeon Films', 'Director': 'Albert Magnoli', 'Genres': ['Music', 'Documentary'], 'Rating': 'PG-13', 'Runtime': '85 min', 'Summary': 'A concert film with theatrical staging, featuring live performances by Prince and his band.'}, 'Planes, Trains & Automobiles_1987': {'Metascore': '72', 'User score': '8.0', 'Title': 'Planes, Trains & Automobiles', 'Release Year': '1987', 'Studio': 'Paramount Pictures', 'Director': 'John Hughes', 'Genres': ['Comedy'], 'Rating': 'R', 'Runtime': '93 min', 'Summary': 'A man must struggle to travel home for Thanksgiving with an obnoxious slob of a shower curtain ring salesman as his only companion.'}, 'Evil Dead II_1987': {'Metascore': '72', 'User score': '8.3', 'Title': 'Evil Dead II', 'Release Year': '1987', 'Studio': 'Rosebud Communications Releasing', 'Director': 'Sam Raimi', 'Genres': ['Mystery', 'Thriller', 'Fantasy', 'Horror', 'Comedy'], 'Rating': 'X', 'Runtime': '84 min', 'Summary': 'Ash (Campbell), the sole survivor of "The Evil Dead," continues his struggle with the forces of the dead, with his girlfriend Linda. The two discover a mysterious tape recorder and hear the voice of Professor Knoby reciting passages from the Necronomicon, or Book of the Dead. The professor\'s words invoke a spell that unleashes the spirit ofAsh (Campbell), the sole survivor of "The Evil Dead," continues his struggle with the forces of the dead, with his girlfriend Linda. The two discover a mysterious tape recorder and hear the voice of Professor Knoby reciting passages from the Necronomicon, or Book of the Dead. The professor\'s words invoke a spell that unleashes the spirit of evil alive in the remote forest surrounding them. Watch in terror as the plot sickens and the supernatural demons invade the cabin on a gory crusade for human victims. In desperation, Ash attempts to escape his horrific fate by battling with the gruesome ghouls with the help of some unexpected visitors. However, he discovers that he is no match for the unspeakable creatures lurking behind every door, and waiting beyond every corner. (Anchor Bay Entertainment)… Expand'}, 'The Stepfather_1987': {'Metascore': '72', 'User score': 'tbd', 'Title': 'The Stepfather', 'Release Year': '1987', 'Studio': 'New Century Vista Film Company', 'Director': 'Joseph Ruben', 'Genres': ['Thriller', 'Horror'], 'Rating': 'R', 'Runtime': '89 min', 'Summary': 'After murdering his entire family, a man marries a widow with a teenage daughter in another town and prepares to do it all over again.'}, 'Barfly_1987': {'Metascore': '70', 'User score': '7.8', 'Title': 'Barfly', 'Release Year': '1987', 'Studio': 'Cannon Group, The', 'Director': 'Barbet Schroeder', 'Genres': ['Drama', 'Comedy', 'Romance'], 'Rating': 'R', 'Runtime': '100 min', 'Summary': 'Based on the life of successful poet Charles Bukowski and his exploits in Hollywood during the 60s, 70s, and 80s.'}, 'Black Widow_1987': {'Metascore': '70', 'User score': 'tbd', 'Title': 'Black Widow', 'Release Year': '1987', 'Studio': 'Twentieth Century Fox', 'Director': 'Bob Rafelson', 'Genres': ['Drama', 'Thriller', 'Crime'], 'Rating': 'TV-14', 'Runtime': '102 min', 'Summary': 'A federal investigator tracks down a gold-digging woman who moves from husband to husband to kill them and collect the inheritance.'}, 'Stakeout_1987': {'Metascore': '69', 'User score': '6.5', 'Title': 'Stakeout', 'Release Year': '1987', 'Studio': 'Buena Vista Pictures Distribution', 'Director': 'John Badham', 'Genres': ['Action', 'Thriller', 'Comedy', 'Crime'], 'Rating': 'R', 'Runtime': '117 min', 'Summary': "Two detectives observe an escaped convict's ex-girlfriend, but complications set in when one of them falls for her."}, 'The Hidden_1987': {'Metascore': '69', 'User score': 'tbd', 'Title': 'The Hidden', 'Release Year': '1987', 'Studio': 'New Line Cinema', 'Director': 'Jack Sholder', 'Genres': ['Sci-Fi', 'Thriller', 'Horror'], 'Rating': 'TV-14', 'Runtime': '97 min', 'Summary': 'A cop and an FBI agent race for answers after law abiding people suddenly become violent criminals.'}, 'Raising Arizona_1987': {'Metascore': '68', 'User score': '8.5', 'Title': 'Raising Arizona', 'Release Year': '1987', 'Studio': 'Twentieth Century Fox Film Corporation', 'Director': 'Ethan Coen', 'Genres': ['Adventure', 'Mystery', 'Comedy', 'Crime'], 'Rating': 'PG-13', 'Runtime': '94 min', 'Summary': "A surreal, hyperactive farce in which a bumbling petty thief and the lady cop who keeps arresting him fall in love and decide to start a family. When they discover they can't have babies, they steal one from a furniture mogul who has just sired a set of quintuplets. The joys of parenthood are soon marred, however, by the difficulties ofA surreal, hyperactive farce in which a bumbling petty thief and the lady cop who keeps arresting him fall in love and decide to start a family. When they discover they can't have babies, they steal one from a furniture mogul who has just sired a set of quintuplets. The joys of parenthood are soon marred, however, by the difficulties of raising an infant on the run. The none-too-bright couple must flee across the southwestern desert in order to elude the villainous biker that has been hired to retrieve the tyke. (20th Century Fox)… Expand"}, 'Swimming to Cambodia_1987': {'Metascore': '68', 'User score': 'tbd', 'Title': 'Swimming to Cambodia', 'Release Year': '1987', 'Studio': 'Cinecom Pictures', 'Director': 'Jonathan Demme', 'Genres': ['Drama'], 'Rating': 'R', 'Runtime': '85 min', 'Summary': 'Spalding Gray\'s stage monologue about his experiences while making "The Killing Fields" and the history of Cambodia.'}, 'Lethal Weapon_1987': {'Metascore': '68', 'User score': '8.5', 'Title': 'Lethal Weapon', 'Release Year': '1987', 'Studio': 'Warner Bros. Pictures', 'Director': 'Richard Donner', 'Genres': ['Action', 'Thriller', 'Crime'], 'Rating': 'R', 'Runtime': '110 min', 'Summary': 'Riggs (Mel Gibson) is a volatile, unregistered risk-taker, known as a "lethal weapon" by his law enforcement colleagues. Murtaugh (Danny Glover) is the conservative detective with a solid reputation. Forced to form an uneasy alliance, they begin to unravel the mystery of an apparent suicide that turns out to be murder -- and more. [Warner Bros.]'}, 'Fatal Attraction_1987': {'Metascore': '67', 'User score': '8.5', 'Title': 'Fatal Attraction', 'Release Year': '1987', 'Studio': 'Paramount Pictures', 'Director': 'Adrian Lyne', 'Genres': ['Drama', 'Thriller'], 'Rating': 'R', 'Runtime': '119 min', 'Summary': 'Fatal Attraction is a story too terrifying to resist, a crackling, tension-packed thriller hinged on the triangle of a man, a wife and vengeful "other woman." This sexy, chic, scary box-office smash grabs hold early-then tops itself with an unforgettably nerve-jolting finale. [Paramount Pictures]'}, 'RoboCop_1987': {'Metascore': '67', 'User score': '8.7', 'Title': 'RoboCop', 'Release Year': '1987', 'Studio': 'Orion Pictures Corporation', 'Director': 'Paul Verhoeven', 'Genres': ['Action', 'Sci-Fi', 'Drama', 'Thriller', 'Crime'], 'Rating': 'R', 'Runtime': '102 min', 'Summary': 'When Officer Alex J. Murphy (Weller) is murdered in a futuristic crime-ridden Detroit, scientists and doctors decide to turn him into a "Robocop". He seeks revenge when memories of his past life return.'}, 'Sammy and Rosie Get Laid_1987': {'Metascore': '67', 'User score': 'tbd', 'Title': 'Sammy and Rosie Get Laid', 'Release Year': '1987', 'Studio': 'Cinecom Pictures', 'Director': 'Stephen Frears', 'Genres': ['Drama', 'Comedy', 'Romance'], 'Runtime': '101 min', 'Summary': "Sammy and Rosie are an unconventional middle-class London married couple. They live in the midst of inner-city chaos, surround themselves with intellectual street people, and sleep with everybody - except each other! Things become interesting when Sammy's father, Raffi, who is a former Indian government minister, comes to London for aSammy and Rosie are an unconventional middle-class London married couple. They live in the midst of inner-city chaos, surround themselves with intellectual street people, and sleep with everybody - except each other! Things become interesting when Sammy's father, Raffi, who is a former Indian government minister, comes to London for a visit. Sammy, Rosie, and Raffi try to find meaning through their lives and loves.… Expand"}, 'The Witches of Eastwick_1987': {'Metascore': '67', 'User score': '7.3', 'Title': 'The Witches of Eastwick', 'Release Year': '1987', 'Studio': 'Warner Bros.', 'Director': 'George Miller', 'Genres': ['Fantasy', 'Horror', 'Comedy'], 'Rating': 'R', 'Runtime': '118 min', 'Summary': 'Three single women in a picturesque village have their wishes granted - at a cost - when a mysterious and flamboyant man enters their lives.'}, 'Innerspace_1987': {'Metascore': '66', 'User score': '8.3', 'Title': 'Innerspace', 'Release Year': '1987', 'Studio': 'Warner Bros.', 'Director': 'Joe Dante', 'Genres': ['Action', 'Adventure', 'Sci-Fi', 'Thriller', 'Fantasy', 'Comedy'], 'Rating': 'PG', 'Runtime': '120 min', 'Summary': 'A hapless store clerk must foil criminals to save the life of the man who, miniaturized in a secret experiment, was accidentally injected into him.'}, 'La Bamba_1987': {'Metascore': '65', 'User score': '7.8', 'Title': 'La Bamba', 'Release Year': '1987', 'Studio': 'Columbia Pictures', 'Director': 'Luis Valdez', 'Genres': ['Biography', 'Drama', 'Music'], 'Rating': 'PG-13', 'Runtime': '108 min', 'Summary': 'Biographical story of the rise from nowhere of early rock and roll singer Ritchie Valens who died at age 17 in a plane crash with Buddy Holly and the Big Bopper.'}, 'Dirty Dancing_1987': {'Metascore': '65', 'User score': '7.6', 'Title': 'Dirty Dancing', 'Release Year': '1987', 'Studio': 'Vestron Pictures', 'Director': 'Emile Ardolino', 'Genres': ['Drama', 'Romance'], 'Rating': 'PG-13', 'Runtime': '100 min', 'Summary': "A young girl (Grey) vacationing with her parents in the conservative Catskills falls for the hotel's maverick dance instructor (Swayze)."}, 'Hamburger Hill_1987': {'Metascore': '64', 'User score': '8.2', 'Title': 'Hamburger Hill', 'Release Year': '1987', 'Studio': 'Paramount Pictures', 'Director': 'John Irvin', 'Genres': ['Action', 'Drama', 'Thriller', 'War'], 'Rating': 'R', 'Runtime': '110 min', 'Summary': 'A very realistic interpretation of one of the bloodiest battles of the Vietnam War.'}, 'The Fourth Protocol_1987': {'Metascore': '64', 'User score': 'tbd', 'Title': 'The Fourth Protocol', 'Release Year': '1987', 'Studio': 'J. Arthur Rank Film Distributors', 'Director': 'John Mackenzie', 'Genres': ['Thriller'], 'Rating': 'R', 'Runtime': '119 min', 'Summary': 'John Preston (Michael Caine) is a British Agent with the task of preventing the Russians detonating a nuclear explosion next to an American base in the UK. The Russians are hoping this will shatter the "special relationship" between the two countries.'}, 'The Lost Boys_1987': {'Metascore': '63', 'User score': '8.7', 'Title': 'The Lost Boys', 'Release Year': '1987', 'Studio': 'Warner Bros. Pictures', 'Director': 'Joel Schumacher', 'Genres': ['Thriller', 'Fantasy', 'Horror', 'Comedy'], 'Rating': 'R', 'Runtime': '97 min', 'Summary': 'Strange events threaten an entire family when two brothers move with their divorced mother to a California town where the local teenage gang turns out to be a pack of vampires. (Warner Bros.)'}, 'Empire of the Sun_1987': {'Metascore': '62', 'User score': '7.8', 'Title': 'Empire of the Sun', 'Release Year': '1987', 'Studio': 'Warner Bros. Pictures', 'Director': 'Steven Spielberg', 'Genres': ['Action', 'Drama', 'History', 'War'], 'Rating': 'PG', 'Runtime': '153 min', 'Summary': "Empire of the Sun—based on J. G. Ballard's autobiographical novel—tells the story of a boy, James Graham, whose privileged life is upturned by the Japanese invasion of Shanghai, December 8, 1941. Separated from his parents, he is eventually captured, and taken to Soo Chow confinement camp, next to a captured Chinese airfield. Amidst theEmpire of the Sun—based on J. G. Ballard's autobiographical novel—tells the story of a boy, James Graham, whose privileged life is upturned by the Japanese invasion of Shanghai, December 8, 1941. Separated from his parents, he is eventually captured, and taken to Soo Chow confinement camp, next to a captured Chinese airfield. Amidst the sickness and food shortages in the camp, Jim attempts to reconstruct his former life, all the while bringing spirit and dignity to those around him. [Warner Bros. Pictures]… Expand"}, 'Back to the Beach_1987': {'Metascore': '62', 'User score': 'tbd', 'Title': 'Back to the Beach', 'Release Year': '1987', 'Studio': 'Paramount Pictures', 'Director': 'Lyndall Hobbs', 'Genres': ['Comedy', 'Musical'], 'Rating': 'PG', 'Runtime': '92 min', 'Summary': "Frankie and Annette, having grown up and put aside their beach-partying lifestyle, visit their daughter in Southern California and discover there's still some wild times left in them."}, 'Dragnet_1987': {'Metascore': '62', 'User score': '5.9', 'Title': 'Dragnet', 'Release Year': '1987', 'Studio': 'Universal Pictures', 'Director': 'Tom Mankiewicz', 'Genres': ['Comedy', 'Crime'], 'Rating': 'PG-13', 'Runtime': '106 min', 'Summary': 'The equally-straight-laced and "by the book" nephew of Joe Friday must work with his more laid-back partner to solve a mystery.'}, 'Project X_1987': {'Metascore': '61', 'User score': 'tbd', 'Title': 'Project X', 'Release Year': '1987', 'Studio': 'Twentieth Century Fox', 'Director': 'Jonathan Kaplan', 'Genres': ['Sci-Fi', 'Drama', 'Thriller', 'Comedy'], 'Rating': 'PG', 'Runtime': '108 min', 'Summary': 'An Air Force pilot (Matthew Broderick) joins a top secret military experiment involving chimps, but begins to suspect there might be something more to the mysterious "Project X".'}, 'Angel Heart_1987': {'Metascore': '61', 'User score': '8.2', 'Title': 'Angel Heart', 'Release Year': '1987', 'Studio': 'TriStar Pictures', 'Director': 'Alan Parker', 'Genres': ['Mystery', 'Thriller', 'Horror'], 'Rating': 'TV-MA', 'Runtime': '113 min', 'Summary': 'A private investigator is hired by a man who calls himself Louis Cyphre to track down a singer named Johnny Favorite. But the investigation takes an unexpected and somber turn.'}, '3 Men and a Baby_1987': {'Metascore': '61', 'User score': '7.5', 'Title': '3 Men and a Baby', 'Release Year': '1987', 'Studio': 'Abril Vídeo', 'Director': 'Leonard Nimoy', 'Genres': ['Drama', 'Comedy', 'Family'], 'Rating': 'TV-PG', 'Runtime': '102 min', 'Summary': "Three bachelors find themselves forced to take care of a baby left by one of the guys' girlfriends."}, 'The Monster Squad_1987': {'Metascore': '61', 'User score': '7.4', 'Title': 'The Monster Squad', 'Release Year': '1987', 'Studio': 'Deliverance Pictures', 'Director': 'Fred Dekker', 'Genres': ['Action', 'Fantasy', 'Horror', 'Comedy'], 'Rating': 'PG-13', 'Runtime': '79 min', 'Summary': 'A young group of monster fanatics attempt to save their hometown from Count Dracula and his monsters.'}, 'The Living Daylights_1987': {'Metascore': '60', 'User score': '6.9', 'Title': 'The Living Daylights', 'Release Year': '1987', 'Studio': 'United Artists', 'Director': 'John Glen', 'Genres': ['Action', 'Adventure', 'Thriller'], 'Rating': 'PG', 'Runtime': '130 min', 'Summary': 'James Bond is living on the edge to stop an evil arms dealer from starting another world war. Bond crosses all seven continents in order to stop the evil Whitaker and General Koskov.'}, 'Cry Freedom_1987': {'Metascore': '59', 'User score': 'tbd', 'Title': 'Cry Freedom', 'Release Year': '1987', 'Studio': 'Universal Pictures', 'Director': 'Richard Attenborough', 'Genres': ['Biography', 'Drama', 'History'], 'Rating': 'PG', 'Runtime': '157 min', 'Summary': 'South African journalist Donald Woods is forced to flee the country, after attempting to investigate the death in custody of his friend, the black activist Steve Biko.'}, 'Crimes of the Heart_1987': {'Metascore': '58', 'User score': 'tbd', 'Title': 'Crimes of the Heart', 'Release Year': '1987', 'Studio': 'De Laurentiis Entertainment Group (DEG)', 'Director': 'Bruce Beresford', 'Genres': ['Drama', 'Comedy'], 'Rating': 'PG-13', 'Runtime': '105 min', 'Summary': 'Three sisters with quite different personalities and lives reunite when Babe, the youngest, has just shot her husband. Oldest sister Lenny takes care of their grandfather and is turning into an old maid. Meg, who aspires to make it in Hollywood as a singer and actress, has had a wild, man-filled life. Their reunion is joyful but also stirsThree sisters with quite different personalities and lives reunite when Babe, the youngest, has just shot her husband. Oldest sister Lenny takes care of their grandfather and is turning into an old maid. Meg, who aspires to make it in Hollywood as a singer and actress, has had a wild, man-filled life. Their reunion is joyful but also stirs up much tension.… Expand'}, 'Hellraiser_1987': {'Metascore': '57', 'User score': '9.1', 'Title': 'Hellraiser', 'Release Year': '1987', 'Studio': 'New World Pictures', 'Director': 'Clive Barker', 'Genres': ['Thriller', 'Horror'], 'Rating': 'R', 'Runtime': '94 min', 'Summary': 'An unfaithful wife encounters the zombie of her dead lover; the demonic cenobites are pursuing him after he escaped their sadomasochistic underworld.'}, 'Born in East L.A._1987': {'Metascore': '57', 'User score': 'tbd', 'Title': 'Born in East L.A.', 'Release Year': '1987', 'Studio': 'CIC Video', 'Director': 'Cheech Marin', 'Genres': ['Comedy'], 'Rating': 'TV-14', 'Runtime': '85 min', 'Summary': 'When a native-born American citizen of Mexican descent is mistakenly deported to Mexico, he has to risk everything to get back home.'}, 'Wall Street_1987': {'Metascore': '56', 'User score': '8.5', 'Title': 'Wall Street', 'Release Year': '1987', 'Studio': 'Twentieth Century Fox Film Corporation', 'Director': 'Oliver Stone', 'Genres': ['Drama', 'Crime'], 'Rating': 'R', 'Runtime': '126 min', 'Summary': "Bud Fox has his sights set on conquering Wall Street.  When legendary broker, Gordon Gekko, takes him under his wing, Fox figures he's on his way. But the road to success is paved with all sorts of corrupt acts that compromise his integrity and sense of self.  Will he be able to get out before it's too late, that is, if Gekko will let him out?"}, 'Throw Momma from the Train_1987': {'Metascore': '56', 'User score': '6.2', 'Title': 'Throw Momma from the Train', 'Release Year': '1987', 'Studio': 'Orion Pictures', 'Director': 'Danny DeVito', 'Genres': ['Thriller', 'Comedy', 'Crime'], 'Rating': 'PG-13', 'Runtime': '88 min', 'Summary': "A bitter ex-husband and a put-upon momma's boy both want their respective former spouse and mother dead. Who will pull it off?"}, 'Some Kind of Wonderful_1987': {'Metascore': '55', 'User score': '8.6', 'Title': 'Some Kind of Wonderful', 'Release Year': '1987', 'Studio': 'Paramount Pictures', 'Director': 'Howard Deutch', 'Genres': ['Drama', 'Romance'], 'Rating': 'PG-13', 'Runtime': '95 min', 'Summary': "Think everyone over 17 has forgotten what it's like to be 16? John Hughes hasn't. Now Hughes delivers another funny, savvy, crowd-pleasing look at adolescence in this story about high school misfit Keith (Stoltz), who falls so head-over-heels for the senior class siren Amanda Jones (Thompson) that he's blind to the charms of his beautifulThink everyone over 17 has forgotten what it's like to be 16? John Hughes hasn't. Now Hughes delivers another funny, savvy, crowd-pleasing look at adolescence in this story about high school misfit Keith (Stoltz), who falls so head-over-heels for the senior class siren Amanda Jones (Thompson) that he's blind to the charms of his beautiful and devoted best pal Watts (Masterson). (Paramount)… Expand"}, 'Light of Day_1987': {'Metascore': '55', 'User score': 'tbd', 'Title': 'Light of Day', 'Release Year': '1987', 'Studio': 'TriStar Pictures', 'Director': 'Paul Schrader', 'Genres': ['Drama', 'Music'], 'Rating': 'PG-13', 'Runtime': '107 min', 'Summary': "The siblings Patty and Joe Rasnick live in an industrial suburb in Cleveland, Ohio. While Patty is focused on their rock band, The Barbusters, Joe also cares for the family and the upbringing of Patty's young son, Benji. Their pious mother reproaches them for their way of life, especially when they quit their jobs and go on tour, takingThe siblings Patty and Joe Rasnick live in an industrial suburb in Cleveland, Ohio. While Patty is focused on their rock band, The Barbusters, Joe also cares for the family and the upbringing of Patty's young son, Benji. Their pious mother reproaches them for their way of life, especially when they quit their jobs and go on tour, taking Benji with them. However, when their mother is dying of terminal cancer, they return home and resolve their problems with her.… Expand"}, 'Dead of Winter_1987': {'Metascore': '55', 'User score': 'tbd', 'Title': 'Dead of Winter', 'Release Year': '1987', 'Studio': 'Metro-Goldwyn-Mayer (MGM)', 'Director': 'Arthur Penn', 'Genres': ['Drama', 'Thriller', 'Horror'], 'Rating': 'R', 'Runtime': '100 min', 'Summary': 'A fledgling actress is lured to a remote mansion for a screen-test, soon discovering she is actually a prisoner in the middle of a blackmail plot.'}, 'Someone to Watch Over Me_1987': {'Metascore': '55', 'User score': 'tbd', 'Title': 'Someone to Watch Over Me', 'Release Year': '1987', 'Studio': 'Columbia Pictures', 'Director': 'Ridley Scott', 'Genres': ['Drama', 'Thriller', 'Romance', 'Crime'], 'Rating': 'R', 'Runtime': '106 min', 'Summary': "A married New York cop falls for the socialite murder witness he's been assigned to protect."}, 'Eddie Murphy: Raw_1987': {'Metascore': '54', 'User score': '8.3', 'Title': 'Eddie Murphy: Raw', 'Release Year': '1987', 'Studio': 'Paramount Pictures', 'Director': 'Robert Townsend', 'Genres': ['Comedy', 'Documentary'], 'Rating': 'R', 'Runtime': '93 min', 'Summary': 'Eddie Murphy in a stand-up performance recorded live. For an hour and a half he talks about his favourite subjects: sex and women.'}, '*batteries not included_1987': {'Metascore': '54', 'User score': '7.3', 'Title': '*batteries not included', 'Release Year': '1987', 'Studio': 'Universal Pictures', 'Director': 'Matthew Robbins', 'Genres': ['Sci-Fi', 'Fantasy', 'Comedy', 'Family'], 'Rating': 'PG', 'Runtime': '106 min', 'Summary': 'Apartment block tenants seek the aid of alien mechanical life-forms to save their building from demolition.'}, 'Baby Boom_1987': {'Metascore': '53', 'User score': '6.1', 'Title': 'Baby Boom', 'Release Year': '1987', 'Studio': 'United Artists', 'Director': 'Charles Shyer', 'Genres': ['Drama', 'Comedy', 'Romance'], 'Rating': 'PG', 'Runtime': '110 min', 'Summary': 'The life of super-yuppie J.C. Wiatt (Diane Keaton) is thrown into turmoil when she inherits a baby from a distant relative.'}, 'Suspect_1987': {'Metascore': '53', 'User score': 'tbd', 'Title': 'Suspect', 'Release Year': '1987', 'Studio': 'TriStar Pictures', 'Director': 'Peter Yates', 'Genres': ['Drama', 'Thriller', 'Crime'], 'Rating': 'TV-14', 'Runtime': '121 min', 'Summary': 'When a homeless man is accused of murdering a Justice Department file clerk, a public defender is tasked with mounting his legal defense.'}, 'Overboard_1987': {'Metascore': '53', 'User score': '8.1', 'Title': 'Overboard', 'Release Year': '1987', 'Studio': 'MGM/UA Distribution Company', 'Director': 'Garry Marshall', 'Genres': ['Comedy', 'Romance'], 'Rating': 'PG', 'Runtime': '106 min', 'Summary': "A cruel and beautiful heiress mocks and cheats over a hired carpenter. When she gets amnesia, he decides to introduce her to working-class life by convincing her they're husband and wife."}, 'Benji the Hunted_1987': {'Metascore': '53', 'User score': 'tbd', 'Title': 'Benji the Hunted', 'Release Year': '1987', 'Studio': 'Buena Vista Pictures', 'Director': 'Joe Camp', 'Genres': ['Adventure', 'Family'], 'Rating': 'G', 'Runtime': '88 min', 'Summary': 'Benji is left in the wilderness after an accident. Will he survive?'}, 'Ishtar_1987': {'Metascore': '52', 'User score': '5.7', 'Title': 'Ishtar', 'Release Year': '1987', 'Studio': 'Columbia Pictures', 'Director': 'Elaine May', 'Genres': ['Adventure', 'Comedy', 'Musical'], 'Rating': 'PG-13', 'Runtime': '107 min', 'Summary': 'Chuck Clarke (Hoffman) and Lyle Rogers (Beatty) are a couple of no-talent New York singer-songwriters who agree to play the only gig they can find at the Chez Casablanca in Morocco. En route, they become embroiled in various international intrigues in the neighboring (fictional) Ishtar.'}, "No Man's Land_1987": {'Metascore': '52', 'User score': 'tbd', 'Title': "No Man's Land", 'Release Year': '1987', 'Studio': 'Orion Pictures', 'Director': 'Peter Werner', 'Genres': ['Drama', 'Thriller', 'Crime'], 'Rating': 'R', 'Runtime': '106 min', 'Summary': 'A rookie police officer goes undercover and infiltrates a car stealing ring.'}, 'Extreme Prejudice_1987': {'Metascore': '51', 'User score': 'tbd', 'Title': 'Extreme Prejudice', 'Release Year': '1987', 'Studio': 'Finnkino', 'Director': 'Walter Hill', 'Genres': ['Action', 'Drama', 'Thriller', 'Crime', 'Western'], 'Rating': 'R', 'Runtime': '96 min', 'Summary': 'A Texas Ranger (Nick Nolte) and a ruthless narcotics kingpin - they were childhood friends, now they are adversaries...'}, 'Prince of Darkness_1987': {'Metascore': '50', 'User score': '8.3', 'Title': 'Prince of Darkness', 'Release Year': '1987', 'Studio': 'Universal Pictures', 'Director': 'John Carpenter', 'Genres': ['Horror'], 'Rating': 'R', 'Runtime': '102 min', 'Summary': 'A research team finds a mysterious cylinder in a deserted church. If opened, it could mean the end of the world.'}, 'A Nightmare on Elm Street 3: Dream Warriors_1987': {'Metascore': '49', 'User score': '8.4', 'Title': 'A Nightmare on Elm Street 3: Dream Warriors', 'Release Year': '1987', 'Studio': 'New Line Cinema', 'Director': 'Chuck Russell', 'Genres': ['Action', 'Thriller', 'Fantasy', 'Horror'], 'Rating': 'R', 'Runtime': '96 min', 'Summary': 'Survivors of undead serial killer Freddy Krueger - who stalks his victims in their dreams - learn to take control of their own dreams in order to fight back.'}, 'Blind Date_1987': {'Metascore': '49', 'User score': '8.2', 'Title': 'Blind Date', 'Release Year': '1987', 'Studio': 'TriStar Pictures', 'Director': 'Blake Edwards', 'Genres': ['Comedy', 'Romance'], 'Rating': 'PG-13', 'Runtime': '95 min', 'Summary': "Walter Davis is a workaholic. His attention is all to his work and very little to his personal life or appearance. Now he needs a date to take to his company's business dinner with a new important Japanese client. His brother sets him up with his wife's cousin Nadia, who is new in town and wants to socialize, but he was warned that if sheWalter Davis is a workaholic. His attention is all to his work and very little to his personal life or appearance. Now he needs a date to take to his company's business dinner with a new important Japanese client. His brother sets him up with his wife's cousin Nadia, who is new in town and wants to socialize, but he was warned that if she gets drunk, she loses control and becomes wild. How will the date turn out - especially when they encounter Nadia's ex-boyfriend David?… Expand"}, 'Less Than Zero_1987': {'Metascore': '48', 'User score': '8.2', 'Title': 'Less Than Zero', 'Release Year': '1987', 'Studio': 'Twentieth Century Fox', 'Director': 'Marek Kanievska', 'Genres': ['Drama', 'Crime'], 'Rating': 'R', 'Runtime': '98 min', 'Summary': "A college kid (Andrew McCarthy) returns to Beverly Hills for the holidays at his ex-girlfriend's (Jami Gertz) request, but he discovers that his old best friend (Robert Downey Jr.) has an out-of-control drug habit and has become beholden to some really bad dudes.  (James Spader et al) Great soundtrack includes tracks by L.L. Cool J. and Poison."}, 'The Big Town_1987': {'Metascore': '48', 'User score': 'tbd', 'Title': 'The Big Town', 'Release Year': '1987', 'Studio': 'Columbia Pictures', 'Director': 'Ben Bolt', 'Genres': ['Drama', 'Thriller', 'Romance'], 'Rating': 'R', 'Runtime': '109 min', 'Summary': "It is 1957. J.C. Cullen is a young man from a small town, with a talent for winning at craps, who leaves for the big city to work as a professional gambler. While there, he breaks the bank at a private craps game at the Gem Club, owned by George Cole, and falls in love with two women, one of them Cole's wife. Infuriated, Cole wagersIt is 1957. J.C. Cullen is a young man from a small town, with a talent for winning at craps, who leaves for the big city to work as a professional gambler. While there, he breaks the bank at a private craps game at the Gem Club, owned by George Cole, and falls in love with two women, one of them Cole's wife. Infuriated, Cole wagers everything on the craps table, including the Gem Club itself, and he and Cullen have it out.… Expand"}, 'Beverly Hills Cop II_1987': {'Metascore': '48', 'User score': '6.6', 'Title': 'Beverly Hills Cop II', 'Release Year': '1987', 'Studio': 'Paramount Pictures', 'Director': 'Tony Scott', 'Genres': ['Action', 'Thriller', 'Comedy', 'Crime'], 'Rating': 'R', 'Runtime': '100 min', 'Summary': 'Axel Foley returns to Beverly Hills to help Taggart and Rosewood investigate the shooting of Chief Bogomil and a series of "alphabet crimes."'}, 'Slam Dance_1987': {'Metascore': '48', 'User score': 'tbd', 'Title': 'Slam Dance', 'Release Year': '1987', 'Studio': 'Island Pictures', 'Director': 'Wayne Wang', 'Genres': ['Mystery', 'Thriller'], 'Rating': 'R', 'Runtime': '92 min', 'Summary': 'An artist, framed for the murder of a woman, is drawn into a web of corruption, blackmail and deceit.'}, 'The Pick-up Artist_1987': {'Metascore': '47', 'User score': '7.5', 'Title': 'The Pick-up Artist', 'Release Year': '1987', 'Studio': 'Twentieth Century Fox', 'Director': 'James Toback', 'Genres': ['Drama', 'Comedy', 'Romance', 'Crime'], 'Rating': 'PG-13', 'Runtime': '81 min', 'Summary': 'A womanizer (Robert Downey Jr.) meets his match when he falls for a woman in debt to the mafia (Molly Ringwald).'}, 'Hiding Out_1987': {'Metascore': '47', 'User score': 'tbd', 'Title': 'Hiding Out', 'Release Year': '1987', 'Studio': 'Cineplex-Odeon Films', 'Director': 'Bob Giraldi', 'Genres': ['Drama', 'Thriller', 'Comedy', 'Romance'], 'Rating': 'PG-13', 'Runtime': '98 min', 'Summary': 'A stockbroker on the run from the mob decides to hide out from them by enrolling as a student in high school.'}, 'Spaceballs_1987': {'Metascore': '46', 'User score': '8.2', 'Title': 'Spaceballs', 'Release Year': '1987', 'Studio': 'Metro-Goldwyn-Mayer (MGM)', 'Director': 'Mel Brooks', 'Genres': ['Adventure', 'Sci-Fi', 'Comedy'], 'Rating': 'Not Rated', 'Runtime': '96 min', 'Summary': 'When the evil Dark Helmet attempts to steal all the air from planet Druidia, a determined Druish Princess, a clueless rogue and a half-man/half-dog creature who\'s his own best friend set out to stop him! But with the forces of darkness closing in on them at ludicrous speed, they\'ll need the help of a wise imp named Yogurt and the mysticalWhen the evil Dark Helmet attempts to steal all the air from planet Druidia, a determined Druish Princess, a clueless rogue and a half-man/half-dog creature who\'s his own best friend set out to stop him! But with the forces of darkness closing in on them at ludicrous speed, they\'ll need the help of a wise imp named Yogurt and the mystical power of "The Schwartz" to bring peace - and merchandising rights - to the entire galaxy! (MGM)… Expand'}, 'The Running Man_1987': {'Metascore': '45', 'User score': '7.2', 'Title': 'The Running Man', 'Release Year': '1987', 'Studio': 'TriStar Pictures', 'Director': 'Paul Michael Glaser', 'Genres': ['Action', 'Sci-Fi', 'Thriller'], 'Rating': 'R', 'Runtime': '101 min', 'Summary': 'A wrongly convicted man must try to survive a public execution gauntlet staged as a game show.'}, 'Predator_1987': {'Metascore': '45', 'User score': '8.7', 'Title': 'Predator', 'Release Year': '1987', 'Studio': 'Twentieth Century Fox Film Corporation', 'Director': 'John McTiernan', 'Genres': ['Action', 'Adventure', 'Sci-Fi', 'Thriller'], 'Rating': 'R', 'Runtime': '107 min', 'Summary': 'Schaefer (Schwarzenegger) and his elite military unit are recruited to rescue hostages held by guerrillas in the Latin American jungle. But once there, they encounter an enemy more deadly than any on earth! (20th Century Fox)'}, 'Adventures in Babysitting_1987': {'Metascore': '45', 'User score': '6.4', 'Title': 'Adventures in Babysitting', 'Release Year': '1987', 'Studio': 'Buena Vista Pictures', 'Director': 'Chris Columbus', 'Genres': ['Action', 'Adventure', 'Thriller', 'Comedy', 'Romance', 'Crime', 'Family'], 'Rating': 'PG-13', 'Runtime': '102 min', 'Summary': "A babysitter must battle her way through the big city after being stranded there with the kids she's looking after."}, 'The Bedroom Window_1987': {'Metascore': '45', 'User score': 'tbd', 'Title': 'The Bedroom Window', 'Release Year': '1987', 'Studio': 'Shochiku-Fuji Company', 'Director': 'Curtis Hanson', 'Genres': ['Mystery', 'Thriller', 'Crime'], 'Rating': 'R', 'Runtime': '112 min', 'Summary': "Terry (Steve Guttenburg) is having an affair with his boss' wife Sylvia (Isabelle Huppert). One night after an office party they are together and Sylvia witnesses an attack on Denise (Elizabeth McGovern) from Terry's bedroom window. She doesn't want to expose their relationship and so is reluctant about talking to the police. Terry, wantingTerry (Steve Guttenburg) is having an affair with his boss' wife Sylvia (Isabelle Huppert). One night after an office party they are together and Sylvia witnesses an attack on Denise (Elizabeth McGovern) from Terry's bedroom window. She doesn't want to expose their relationship and so is reluctant about talking to the police. Terry, wanting to help, gives the police the description of the attacker. He soon becomes the main suspect in the case. He then sets to find the real rapist/killer with some help from victim Denise.… Expand"}, 'Dogs in Space_1987': {'Metascore': '43', 'User score': 'tbd', 'Title': 'Dogs in Space', 'Release Year': '1987', 'Studio': 'Skouras Pictures', 'Director': 'Richard Lowenstein', 'Genres': ['Drama'], 'Rating': 'R', 'Runtime': '103 min', 'Summary': "The film is set in a house occupied by a collection of social misfits. The main storyline is that of a strange musician's relationship with a girl, their drug use and his band. These events are surrounded by a chaotic myriad of sub-plots. A homicidal chainsaw maniac's lust for his machine and a T.V station's offer of money in return for aThe film is set in a house occupied by a collection of social misfits. The main storyline is that of a strange musician's relationship with a girl, their drug use and his band. These events are surrounded by a chaotic myriad of sub-plots. A homicidal chainsaw maniac's lust for his machine and a T.V station's offer of money in return for a piece of the Skylab satellite that has fallen to earth are just two. The film is composed of small fragments in the lives of its inhabitants, each following onto the next, sometimes overlapping and ending in tragedy.… Expand"}, 'Russkies_1987': {'Metascore': '43', 'User score': 'tbd', 'Title': 'Russkies', 'Release Year': '1987', 'Studio': 'New Century Vista Film Company', 'Director': 'Rick Rosenthal', 'Genres': ['Drama'], 'Rating': 'PG', 'Runtime': '99 min', 'Summary': 'A group of American boys and a shipwrecked Russian sailor become friends in the midst of the Cold War.'}, 'Amazon Women on the Moon_1987': {'Metascore': '42', 'User score': 'tbd', 'Title': 'Amazon Women on the Moon', 'Release Year': '1987', 'Studio': 'Barcino Films', 'Director': 'Carl Gottlieb', 'Genres': ['Sci-Fi', 'Comedy'], 'Rating': 'R', 'Runtime': '85 min', 'Summary': 'A spoof 1950s science fiction movie, interspersed with various comedy sketches concerning late night television.'}, 'Harry and the Hendersons_1987': {'Metascore': '42', 'User score': '6.5', 'Title': 'Harry and the Hendersons', 'Release Year': '1987', 'Studio': 'Universal Pictures', 'Director': 'William Dear', 'Genres': ['Fantasy', 'Comedy', 'Family'], 'Rating': 'PG', 'Runtime': '110 min', 'Summary': "Returning from a hunting trip in the forest, the Henderson family's car hits an animal in the road. When they examine the body, they find it's a Bigfoot. Thinking it might be dead, they decide to take it home.  But when the Bigfoot wakes up, it turns out he is far from the ferocious monster they feared.  He's a friendly giant. In theirReturning from a hunting trip in the forest, the Henderson family's car hits an animal in the road. When they examine the body, they find it's a Bigfoot. Thinking it might be dead, they decide to take it home.  But when the Bigfoot wakes up, it turns out he is far from the ferocious monster they feared.  He's a friendly giant. In their attempts to keep Harry a secret, the Henderson's have to hide him from the authorities and a man who has made it his goal in life to catch a Bigfoot.… Expand"}, 'Over the Top_1987': {'Metascore': '40', 'User score': '7.3', 'Title': 'Over the Top', 'Release Year': '1987', 'Studio': 'Warner Bros.', 'Director': 'Menahem Golan', 'Genres': ['Action', 'Drama', 'Sport'], 'Rating': 'PG', 'Runtime': '93 min', 'Summary': 'Tough trucker Lincoln Hawk is determined to win back his son and triumph at the world arm wrestling championships.'}, 'The Believers_1987': {'Metascore': '40', 'User score': 'tbd', 'Title': 'The Believers', 'Release Year': '1987', 'Studio': 'Orion Pictures', 'Director': 'John Schlesinger', 'Genres': ['Drama', 'Mystery', 'Thriller', 'Horror', 'Crime'], 'Rating': 'R', 'Runtime': '114 min', 'Summary': 'A New York psychiatrist finds that a brujería-inspired cult, which believes in child sacrifice, has a keen interest in his own son.'}, 'Creepshow 2_1987': {'Metascore': '39', 'User score': '6.0', 'Title': 'Creepshow 2', 'Release Year': '1987', 'Studio': 'New World Pictures', 'Director': 'Michael Gornick', 'Genres': ['Fantasy', 'Horror', 'Comedy'], 'Rating': 'R', 'Runtime': '92 min', 'Summary': 'Three more bone-chilling tales that include a vengeful wooden Native American, a monstrous blob in a lake, and a hitchhiker who wants revenge and will not die.'}, 'Wisdom_1987': {'Metascore': '37', 'User score': 'tbd', 'Title': 'Wisdom', 'Release Year': '1987', 'Studio': 'Twentieth Century Fox', 'Director': 'Emilio Estevez', 'Genres': ['Drama', 'Crime'], 'Rating': 'R', 'Runtime': '109 min', 'Summary': 'Unable to find work after a past felony, graduate John Wisdom and his girlfriend embark on a cross-country bank-robbing spree in order to aid American farmers.'}, 'The Principal_1987': {'Metascore': '37', 'User score': 'tbd', 'Title': 'The Principal', 'Release Year': '1987', 'Studio': 'TriStar Pictures', 'Director': 'Christopher Cain', 'Genres': ['Action', 'Drama', 'Thriller', 'Crime'], 'Rating': 'R', 'Runtime': '109 min', 'Summary': 'A teacher is assigned to be the principal of a violent and crime-ridden high school.'}, "Three O'Clock High_1987": {'Metascore': '36', 'User score': '7.5', 'Title': "Three O'Clock High", 'Release Year': '1987', 'Studio': 'Universal Pictures', 'Director': 'Phil Joanou', 'Genres': ['Comedy'], 'Rating': 'PG-13', 'Runtime': '97 min', 'Summary': "A nerd gets himself in hot water with the new bully, a quiet bad boy who challenges him to fight on the grounds of their high school after the day's end."}, 'The Secret of My Succe$s_1987': {'Metascore': '36', 'User score': '7.5', 'Title': 'The Secret of My Succe$s', 'Release Year': '1987', 'Studio': 'Universal Pictures', 'Director': 'Herbert Ross', 'Genres': ['Comedy', 'Romance'], 'Rating': 'PG-13', 'Runtime': '111 min', 'Summary': "A talented young man can't get an executive position without rising through the ranks, so he comes up with a shortcut, which also benefits his love life."}, "Can't Buy Me Love_1987": {'Metascore': '36', 'User score': '8.0', 'Title': "Can't Buy Me Love", 'Release Year': '1987', 'Studio': 'Buena Vista Pictures', 'Director': 'Steve Rash', 'Genres': ['Drama', 'Comedy', 'Romance'], 'Rating': 'PG-13', 'Runtime': '94 min', 'Summary': 'A nerdy outcast secretly pays the most popular girl in school one thousand dollars to be his girlfriend.'}, 'Masters of the Universe_1987': {'Metascore': '35', 'User score': '4.2', 'Title': 'Masters of the Universe', 'Release Year': '1987', 'Studio': 'Cannon Group, The', 'Director': 'Gary Goddard', 'Genres': ['Action', 'Adventure', 'Sci-Fi', 'Thriller', 'Fantasy'], 'Rating': 'PG', 'Runtime': '106 min', 'Summary': 'The heroic warrior He-Man battles against lord Skeletor and his armies of darkness for control of Castle Grayskull.'}, 'The Hanoi Hilton_1987': {'Metascore': '32', 'User score': 'tbd', 'Title': 'The Hanoi Hilton', 'Release Year': '1987', 'Studio': 'Cannon Film Distributors', 'Director': 'Lionel Chetwynd', 'Genres': ['Drama', 'War'], 'Rating': 'R', 'Runtime': '125 min', 'Summary': "A drama focusing on the suffering, torture, and brutal treatment the American P.O.W.s had to deal with daily while in North Vietnam's Hoa Lo Prison, the most infamous P.O.W. camp in Hanoi. The film focuses on the resistance the prisoners gave to their captors and the strong bonds formed by the Americans during their captivity."}, 'House II: The Second Story_1987': {'Metascore': '31', 'User score': 'tbd', 'Title': 'House II: The Second Story', 'Release Year': '1987', 'Studio': 'New World Pictures', 'Director': 'Ethan Wiley', 'Genres': ['Fantasy', 'Horror', 'Comedy'], 'Rating': 'PG-13', 'Runtime': '88 min', 'Summary': 'The new owner of a sinister house gets involved with reanimated corpses and demons searching for an ancient Aztec skull with magic powers.'}, 'North Shore_1987': {'Metascore': '31', 'User score': 'tbd', 'Title': 'North Shore', 'Release Year': '1987', 'Studio': 'Universal Pictures', 'Director': 'William Phelps', 'Genres': ['Action', 'Drama', 'Sport', 'Romance'], 'Rating': 'PG', 'Runtime': '96 min', 'Summary': 'An Arizona surfer, laughed at by the veteran boarders in Hawaii, finds a mentor - and a girlfriend - and prepares to ride the giant waves of Oahu.'}, 'Hot Pursuit_1987': {'Metascore': '29', 'User score': 'tbd', 'Title': 'Hot Pursuit', 'Release Year': '1987', 'Studio': 'Filmpac Distribution', 'Director': 'Steven Lisberger', 'Genres': ['Comedy'], 'Rating': 'PG-13', 'Runtime': '93 min', 'Summary': "Dan (John Cusack) arrives too late for his girlfriend's family's plane to the Caribbean. He gets the next. Once there, Dan's hot pursuit of his girlfriend includes 3 friendly locals, a dubious yacht skipper, corrupt police, hijacker, pirates etc."}, 'Revenge of the Nerds II: Nerds in Paradise_1987': {'Metascore': '28', 'User score': 'tbd', 'Title': 'Revenge of the Nerds II: Nerds in Paradise', 'Release Year': '1987', 'Studio': 'Twentieth Century Fox', 'Director': 'Joe Roth', 'Genres': ['Comedy'], 'Rating': 'TV-14', 'Runtime': '88 min', 'Summary': 'The rising college nerds set out to a convention in Florida, but are not welcomed by the Alpha Beta representatives. '}, 'Surf Nazis Must Die_1987': {'Metascore': '28', 'User score': 'tbd', 'Title': 'Surf Nazis Must Die', 'Release Year': '1987', 'Studio': 'Troma Entertainment', 'Director': 'Peter George', 'Genres': ['Action', 'Drama', 'Horror', 'Comedy'], 'Rating': 'R', 'Runtime': '83 min', 'Summary': 'When the son of a gun-wielding woman is murdered by neo-Nazi surf punks in the post-apocalyptic future, his Mama hunts them down for some bloodthirsty revenge.'}, 'Summer School_1987': {'Metascore': '27', 'User score': '6.9', 'Title': 'Summer School', 'Release Year': '1987', 'Studio': 'Paramount Pictures', 'Director': 'Carl Reiner', 'Genres': ['Comedy', 'Romance'], 'Rating': 'PG-13', 'Runtime': '97 min', 'Summary': 'High School coach Freddy Shoop (Harmon) has plans: summer in Hawaii. But the vice-principal has plans for Freddy, too. Teaching remedial English. Aloha, paradise. Hello, Summer School. Shoop and his class of likable misfits survive goof-ups and good times on the road to academic success... and the self-respect that goes with it. (Paramount)'}, "Who's That Girl_1987": {'Metascore': '27', 'User score': '8.6', 'Title': "Who's That Girl", 'Release Year': '1987', 'Studio': 'Warner Bros.', 'Director': 'James Foley', 'Genres': ['Comedy', 'Romance', 'Music'], 'Rating': 'PG', 'Runtime': '92 min', 'Summary': 'The life of an uptight tax lawyer turns chaotic when he is asked to escort a young woman newly released from prison, who persuades him to help prove her innocence.'}, 'Police Academy 4: Citizens on Patrol_1987': {'Metascore': '26', 'User score': '6.8', 'Title': 'Police Academy 4: Citizens on Patrol', 'Release Year': '1987', 'Studio': 'Warner Bros.', 'Director': 'Jim Drake', 'Genres': ['Comedy', 'Crime'], 'Rating': 'TV-PG', 'Runtime': '88 min', 'Summary': 'The motley crew of Police Academy graduates have been assigned to train a group of civilian volunteers to fight crime that is plaguing their streets.'}, 'Flowers in the Attic_1987': {'Metascore': '25', 'User score': 'tbd', 'Title': 'Flowers in the Attic', 'Release Year': '1987', 'Studio': 'New World Pictures', 'Director': 'Jeffrey Bloom', 'Genres': ['Drama', 'Mystery', 'Thriller'], 'Rating': 'PG-13', 'Runtime': '93 min', 'Summary': 'Children are hidden away in the attic by their conspiring mother and grandmother.'}, 'Superman IV: The Quest for Peace_1987': {'Metascore': '24', 'User score': '2.6', 'Title': 'Superman IV: The Quest for Peace', 'Release Year': '1987', 'Studio': 'Warner Bros', 'Director': 'Sidney J. Furie', 'Genres': ['Action', 'Adventure', 'Sci-Fi', 'Fantasy', 'Family'], 'Rating': 'PG', 'Runtime': '91 min', 'Summary': "The Man of Steel crusades for nuclear disarmament and meets Lex Luthor's latest creation, Nuclear Man."}, 'Mannequin_1987': {'Metascore': '21', 'User score': '8.0', 'Title': 'Mannequin', 'Release Year': '1987', 'Studio': 'Twentieth Century Fox', 'Director': 'Michael Gottlieb', 'Genres': ['Fantasy', 'Comedy', 'Romance'], 'Rating': 'PG', 'Runtime': '90 min', 'Summary': 'A young artist, searching for his vocation, makes a mannequin so perfect he falls in love with it. Finding the mannequin in a store window, he gets a job there and his creation comes to life.'}, 'Jaws: The Revenge_1987': {'Metascore': '15', 'User score': '3.6', 'Title': 'Jaws: The Revenge', 'Release Year': '1987', 'Studio': 'Universal Pictures', 'Director': 'Joseph Sargent', 'Genres': ['Adventure', 'Thriller', 'Horror'], 'Rating': 'TV-14', 'Runtime': '89 min', 'Summary': "Chief Brody's widow believes that her family is deliberately being targeted by another shark in search of revenge."}, 'The Garbage Pail Kids Movie_1987': {'Metascore': '1', 'User score': '0.7', 'Title': 'The Garbage Pail Kids Movie', 'Release Year': '1987', 'Studio': 'Atlantic Releasing Corporation', 'Director': 'Rod Amateau', 'Genres': ['Adventure', 'Fantasy', 'Comedy', 'Family', 'Musical'], 'Rating': 'PG', 'Runtime': '100 min', 'Summary': 'Dodger must confront the struggles of life as he is visited by the Garbage Pail Kids and intimidated by some older bullies.'}}

my_df = pd.DataFrame(columns=['Title', 'Metascore', 'User score', 'Release Year', 'Studio',
                              'Director', 'Rating', 'Genres', 'Runtime', 'Summary'])
for key, item in sample_dictionary.items():
    new_key = key
    series = pd.Series(sample_dictionary[key])
    series.name = new_key
    my_df = my_df.append(series)
my_df = my_df.replace(np.nan, None, regex=True)


class Database:
    def __init__(self):
        """
        Initialisation function for Database class.
        """
        self.db_name = cfg.DATABASE_NAME
        self.con, self.cur = self.connect_to_db()
        # self.con, self.cur updated after DB confirmed/created
        self.con, self.cur = self.create_db()
        self.create_tables_db()

    def connect_to_db(self):
        """
        Creates initial connection and cursor objects.
        :return: con and cursor
        """
        # Create initial connection object.
        con = pymysql.connect(host='localhost', user='root',
                              password=cfg.PASSWORD_DB_SERVER, cursorclass=pymysql.cursors.DictCursor)
        # Create initial cursor
        cur = con.cursor()
        return con, cur

    def create_db(self):
        """
        Checks if DB exists. If not, creates it.
        :return: con, cursor
        """
        query = f"CREATE DATABASE IF NOT EXISTS {self.db_name}"
        self.cur.execute(query)
        # Update con with confirmed/new DB info
        con = pymysql.connect(host='localhost', user='root', password=cfg.PASSWORD_DB_SERVER,
                              database=self.db_name, cursorclass=pymysql.cursors.DictCursor)
        # Updated cursor
        cur = con.cursor()
        return con, cur

    def create_tables_db(self):
        """
        Assembles tables as required if don't exist in self.db_name
        :return: None
        """

        self.cur.execute("""   
                            CREATE TABLE IF NOT EXISTS studios (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255)
                            );""")

        self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS directors (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255)
                            );""")

        self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS genres (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255)
                            );""")

        self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS creators (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255)
                            );""")

        self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS platforms (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255)
                            );""")

        self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS movies (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255) NOT NULL,
                            unique_identifier varchar(500) NOT NULL,
                            meta_score varchar(255),
                            user_score varchar(255),
                            release_year varchar(255),
                            rating varchar(255),
                            runtime varchar(255),
                            summary varchar(10000),
                            studio_id int,
                            director_id int,
                            FOREIGN KEY(studio_id) REFERENCES studios(id),
                            FOREIGN KEY(director_id) REFERENCES directors(id)
                            );""")

        self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS movies_genres (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            movie_id int,
                            genre_id int,
                            FOREIGN KEY(movie_id) REFERENCES movies(id),
                            FOREIGN KEY(genre_id) REFERENCES genres(id)
                            );""")

        self.cur.execute("""    
                            CREATE TABLE IF NOT EXISTS tv_shows (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255) NOT NULL,
                            unique_identifier varchar(255) NOT NULL,
                            meta_score varchar(255),
                            user_score varchar(255),
                            release_date varchar(255),
                            summary varchar(10000),
                            studio_id int,
                            creator_id int,
                            FOREIGN KEY(studio_id) REFERENCES studios(id),
                            FOREIGN KEY(creator_id) REFERENCES creators(id)
                            );""")

        self.cur.execute("""
                            CREATE TABLE IF NOT EXISTS tv_shows_genres (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            tv_show_id int,
                            genre_id int,
                            FOREIGN KEY(tv_show_id) REFERENCES tv_shows(id),
                            FOREIGN KEY(genre_id) REFERENCES genres(id)    
                            );""")

        self.cur.execute("""
                            CREATE TABLE IF NOT EXISTS games (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            name varchar(255) NOT NULL,
                            unique_identifier varchar(255) NOT NULL,
                            meta_score varchar(255),
                            user_score varchar(255),
                            release_date varchar(255),
                            rating varchar(255),
                            summary varchar(10000),
                            studio_id int,
                            FOREIGN KEY(studio_id) REFERENCES studios(id)
                            );""")

        self.cur.execute("""
                            CREATE TABLE IF NOT EXISTS games_genres (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            game_id int,
                            genre_id int,
                            FOREIGN KEY(game_id) REFERENCES games(id),
                            FOREIGN KEY(genre_id) REFERENCES genres(id)
                            );""")

        self.cur.execute("""                          
                            CREATE TABLE IF NOT EXISTS games_platforms (
                            id int PRIMARY KEY NOT NULL AUTO_INCREMENT,
                            game_id int,
                            platform_id int,
                            FOREIGN KEY(game_id) REFERENCES games(id),
                            FOREIGN KEY(platform_id) REFERENCES platforms(id)  
                            );""")

    def add_to_database_by_type(self, container, container_type):
        """
        Checks the type of the Dataframe and calls the correct method to add to the
        database (movies, tv shows, games).
        :param container: Dataframe
        :param container_type: Type of dataframe
        :return: None
        """
        if container_type == 'movies':
            self.populate_tables_movies(container)
            logging.info(f'Finished populating new container to database.')
        elif container_type == 'tv':
            self.populate_tables_tv_shows(container)
            logging.info(f'Finished populating new container to database.')
        elif container_type == 'games':
            self.populate_tables_games(container)
            logging.info(f'Finished populating new container to database.')
        else:
            logging.error(f'Failed to add to database.')

    def populate_tables_movies(self, container):
        """
        Takes in a pd Dataframe. Each row series contains all information for a movie item.
        Inserts the data from the df to the database.
        :param self:
        :param container: pd DataFrame.
        :return: None
        """
        counter = 0
        for index, row_df in container.iterrows():
            # print(row_df)
            # print(row_num)
            unique_identifier = index
            # Check if movie already in table
            self.cur.execute(f"""SELECT unique_identifier as unique_identifier 
                                 FROM movies WHERE unique_identifier="{unique_identifier}";""")
            duplicate_item_check_query = self.cur.fetchone()
            # if duplicate_item_check_query:
            #     movie_id = duplicate_item_check_query['unique_identifier']
            #     print(movie_id)
            # else:
            #     movie_id = []
            if duplicate_item_check_query:
                continue
            self.cur.execute(f"""SELECT id as id FROM studios WHERE name="{row_df['Studio']}";""")
            studio_existence_query = self.cur.fetchone()
            if studio_existence_query is None:
                # If studio not in studio table, insert it and then select the id to use for movies FK.
                self.cur.execute(f"""INSERT INTO studios (name) VALUES ("{row_df['Studio']}");""")
                self.cur.execute(f"""SELECT id AS id FROM studios WHERE name="{row_df['Studio']}";""")
                studio_existence_query = self.cur.fetchone()
                studio_id = studio_existence_query['id']
            else:
                studio_id = studio_existence_query['id']
            self.cur.execute(f"""SELECT id AS id FROM directors WHERE name="{row_df['Director']}";""")
            director_existence_query = self.cur.fetchone()
            if director_existence_query is None:
                self.cur.execute(f"""INSERT INTO directors (name) VALUES ("{row_df['Director']}");""")
                self.cur.execute(f"""SELECT id AS id FROM directors WHERE name="{row_df['Director']}";""")
                director_existence_query = self.cur.fetchone()
                director_id = director_existence_query['id']
            else:
                director_id = director_existence_query['id']
            sql_to_execute = fr"""INSERT INTO movies (name, unique_identifier, meta_score, user_score, release_year,
                                rating, runtime, summary, studio_id, director_id) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
            # print(sql_to_execute)
            # Insert record into movies table
            self.cur.execute(sql_to_execute, (row_df['Title'], unique_identifier, row_df['Metascore'],
                             row_df['User score'], row_df['Release Year'], row_df['Rating'],
                             row_df['Runtime'], row_df['Summary'], studio_id, director_id))

            self.cur.execute(f"""SELECT id FROM movies WHERE unique_identifier="{unique_identifier}";""")
            movie_id_query = self.cur.fetchone()
            movie_id = movie_id_query['id']
            # Insert into movies_genres table
            for genre in row_df['Genres']:
                self.cur.execute(f"""SELECT id FROM genres WHERE name="{genre}";""")
                genre_id_query = self.cur.fetchone()
                if genre_id_query is None:
                    self.cur.execute(f"""INSERT INTO genres (name) VALUES ("{genre}");""")
                    self.cur.execute(f"""SELECT id FROM genres WHERE name="{genre}";""")
                    genre_id_query = self.cur.fetchone()
                    genre_id = genre_id_query['id']
                else:
                    genre_id = genre_id_query['id']
                # Insert this movie, genre combination into movies_genres table
                self.cur.execute(f"""INSERT INTO movies_genres (movie_id, genre_id) VALUES ({movie_id}, {genre_id});""")
            counter += 1
            # if counter % cfg.SIZE_OF_COMMIT == 0 or counter == len(container) - 1:
            self.con.commit()
            logging.debug(f'Commit of {cfg.SIZE_OF_COMMIT} entries')
            logging.debug(f'Item {unique_identifier} entries')

    def populate_tables_tv_shows(self, container):
        """
        Takes in a pd Dataframe. Each row series contains all information for a tv_show item.
        Inserts the data from the df to the database.
        :param self:
        :param container: pd DataFrame.
        :return: None
        """
        counter = 0
        for index, row_df in container.iterrows():
            # print(row_df)
            # print(row_num)
            unique_identifier = index
            # Check if movie already in table
            self.cur.execute(f"""SELECT unique_identifier as unique_identifier 
                                 FROM movies WHERE unique_identifier="{unique_identifier}";""")
            duplicate_item_check_query = self.cur.fetchone()
            # if duplicate_item_check_query:
            #     movie_id = duplicate_item_check_query['unique_identifier']
            #     print(movie_id)
            # else:
            #     movie_id = []
            if duplicate_item_check_query:
                continue
            self.cur.execute(f"""SELECT id as id FROM studios WHERE name="{row_df['Studio']}";""")
            studio_existence_query = self.cur.fetchone()
            if studio_existence_query is None:
                # If studio not in studio table, insert it and then select the id to use for movies FK.
                self.cur.execute(f"""INSERT INTO studios (name) VALUES ("{row_df['Studio']}");""")
                self.cur.execute(f"""SELECT id AS id FROM studios WHERE name="{row_df['Studio']}";""")
                studio_existence_query = self.cur.fetchone()
                studio_id = studio_existence_query['id']
            else:
                studio_id = studio_existence_query['id']
            self.cur.execute(f"""SELECT id AS id FROM creators WHERE name="{row_df['Creator']}";""")
            creator_existence_query = self.cur.fetchone()
            if creator_existence_query is None:
                self.cur.execute(f"""INSERT INTO creators (name) VALUES ("{row_df['Creator']}");""")
                self.cur.execute(f"""SELECT id AS id FROM creators WHERE name="{row_df['Creator']}";""")
                creator_existence_query = self.cur.fetchone()
                creator_id = creator_existence_query['id']
            else:
                creator_id = creator_existence_query['id']
            sql_to_execute = fr"""INSERT INTO tv_shows (name, unique_identifier, meta_score, user_score, release_date,
                                summary, studio_id, creator_id) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""
            # print(sql_to_execute)
            # Insert record into movies table
            self.cur.execute(sql_to_execute, (row_df['Title'], unique_identifier, row_df['Metascore'],
                             row_df['User score'], row_df['Release Year'], row_df['Summary'],
                             studio_id, creator_id))

            self.cur.execute(f"""SELECT id FROM tv_shows WHERE unique_identifier="{unique_identifier}";""")
            tv_show_query = self.cur.fetchone()
            tv_show_id = tv_show_query['id']
            # Insert into tv_shows_genres table
            for genre in row_df['Genres']:
                self.cur.execute(f"""SELECT id FROM genres WHERE name="{genre}";""")
                genre_id_query = self.cur.fetchone()
                if genre_id_query is None:
                    self.cur.execute(f"""INSERT INTO genres (name) VALUES ("{genre}");""")
                    self.cur.execute(f"""SELECT id FROM genres WHERE name="{genre}";""")
                    genre_id_query = self.cur.fetchone()
                    genre_id = genre_id_query['id']
                else:
                    genre_id = genre_id_query['id']
                # Insert this tv_show, genre combination into tv_shows_genres table
                self.cur.execute(f"""INSERT INTO tv_shows_genres (tv_show_id, genre_id) VALUES ({tv_show_id}, {genre_id});""")
            counter += 1
            # if counter % cfg.SIZE_OF_COMMIT == 0 or counter == len(container) - 1:
            self.con.commit()
            logging.debug(f'Commit of {cfg.SIZE_OF_COMMIT} entries')
            logging.debug(f'Item {unique_identifier} entries')


    def populate_tables_games(self, container):
        """
        Takes in a pd Dataframe. Each row series contains all information for a game item.
        Inserts the data from the df to the database.
        :param self:
        :param container: pd DataFrame.
        :return: None
        """
        counter = 0
        for index, row_df in container.iterrows():
            # print(row_df)
            # print(row_num)
            unique_identifier = index
            # Check if movie already in table
            self.cur.execute(f"""SELECT unique_identifier as unique_identifier 
                                 FROM games WHERE unique_identifier="{unique_identifier}";""")
            duplicate_item_check_query = self.cur.fetchone()
            # if duplicate_item_check_query:
            #     movie_id = duplicate_item_check_query['unique_identifier']
            #     print(movie_id)
            # else:
            #     movie_id = []
            if duplicate_item_check_query:
                continue
            self.cur.execute(f"""SELECT id as id FROM studios WHERE name="{row_df['Studio']}";""")
            studio_existence_query = self.cur.fetchone()
            if studio_existence_query is None:
                # If studio not in studio table, insert it and then select the id to use for games FK.
                self.cur.execute(f"""INSERT INTO studios (name) VALUES ("{row_df['Studio']}");""")
                self.cur.execute(f"""SELECT id AS id FROM studios WHERE name="{row_df['Studio']}";""")
                studio_existence_query = self.cur.fetchone()
                studio_id = studio_existence_query['id']
            else:
                studio_id = studio_existence_query['id']
            sql_to_execute = fr"""INSERT INTO games (name, unique_identifier, meta_score, user_score, release_date,
                                rating, summary, studio_id) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""
            # print(sql_to_execute)
            # Insert record into movies table
            self.cur.execute(sql_to_execute, (row_df['Title'], unique_identifier, row_df['Metascore'],
                             row_df['User score'], row_df['Release Year'], row_df['Rating'], row_df['Summary'],
                             studio_id))

            self.cur.execute(f"""SELECT id FROM games WHERE unique_identifier="{unique_identifier}";""")
            game_id_query = self.cur.fetchone()
            game_id = game_id_query['id']
            # Insert into games_genres table
            for genre in row_df['Genres']:
                self.cur.execute(f"""SELECT id FROM genres WHERE name="{genre}";""")
                genre_id_query = self.cur.fetchone()
                if genre_id_query is None:
                    self.cur.execute(f"""INSERT INTO genres (name) VALUES ("{genre}");""")
                    self.cur.execute(f"""SELECT id FROM genres WHERE name="{genre}";""")
                    genre_id_query = self.cur.fetchone()
                    genre_id = genre_id_query['id']
                else:
                    genre_id = genre_id_query['id']
                # Insert this game, genre combination into games_genres table
                self.cur.execute(f"""INSERT INTO games_genres (game_id, genre_id) VALUES ({game_id}, {genre_id});""")
            # Insert into games_platforms table
            for platform in row_df['Platform']:
                self.cur.execute(f"""SELECT id FROM platforms WHERE name="{platform}";""")
                platform_id_query = self.cur.fetchone()
                if platform_id_query is None:
                    self.cur.execute(f"""INSERT INTO platforms (name) VALUES ("{platform}");""")
                    self.cur.execute(f"""SELECT id FROM platforms WHERE name="{platform}";""")
                    platform_id_query = self.cur.fetchone()
                    platform_id = platform_id_query['id']
                else:
                    platform_id = platform_id_query['id']
                # Insert this game, platform combination into games_platforms table
                self.cur.execute(f"""INSERT INTO games_platforms (game_id, platform_id) VALUES ({game_id}, {platform_id});""")
            counter += 1
            # if counter % cfg.SIZE_OF_COMMIT == 0 or counter == len(container) - 1:
            self.con.commit()
            logging.debug(f'Commit of {cfg.SIZE_OF_COMMIT} entries')
            logging.debug(f'Item {unique_identifier} entries')



def main():
    # df_tv_shows = cl.tv_show('year', '2002')
    # print(df_tv_shows.columns)
    # db1.populate_tables_tv_shows(df_tv_shows)
    # df_game = cl.game('year', '1996')
    # df_game = df_game.replace(np.nan, "missing", regex=True)
    db1 = Database()
    assert db1.db_name == 'metacritic'
    db1.connect_to_db()
    db1.create_db()
    db1.create_tables_db()
    db1.populate_tables_games(df_game)
    print(db1.db_name)



if __name__ == '__main__':
    main()
