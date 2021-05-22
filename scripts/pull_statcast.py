import logging

from data_pull_classes import MultiYearDataPull, StatcastDataPull
from pybaseball import cache
from pybaseball.statcast_fielding import (statcast_catcher_framing,
                                          statcast_catcher_poptime,
                                          statcast_outfield_catch_prob,
                                          statcast_outfield_directional_oaa,
                                          statcast_outfielder_jump,
                                          statcast_outs_above_average)
from utils import configure_logging


def main():
    cache.enable()

    logging.info('Beginning to update statcast data')
    _statcast = StatcastDataPull(limit=10)
    _statcast.update_table()

    _catcher_frame = MultiYearDataPull('statcast_catcher_framing', 'statcast', statcast_catcher_framing, 2008, limit=5, 
                                    current_year=True, kwargs={'min_called_p':0})
    _catcher_frame.update_table()
    
    _catcher_pop = MultiYearDataPull('statcast_catcher_poptime', 'statcast', statcast_catcher_poptime, 2008, limit=5, 
                                    current_year=True, kwargs={'min_2b_att':0, 'min_3b_att':0}, add_year=True)
    _catcher_pop.update_table()

    _outs_above_average = MultiYearDataPull('statcast_outs_above_average', 'statcast', statcast_outs_above_average, 2008, limit=5, 
                                    current_year=True,add_year=True, kwargs={'min_att':0})
    _outs_above_average.update_table()

    _outfield_catch_prob = MultiYearDataPull('statcast_outfield_catch_prob', 'statcast', statcast_outfield_catch_prob, 2008, limit=5, 
                                    current_year=True, kwargs={'min_opp':0}, add_year=True)
    _outfield_catch_prob.update_table()

    _outfielder_jump = MultiYearDataPull('statcast_outfielder_jump', 'statcast', statcast_outfielder_jump, 2008, limit=5, 
                                    current_year=True, kwargs={'min_att':0}, add_year=True)
    _outfielder_jump.update_table()

    _outfielder_directional = MultiYearDataPull('statcast_outfield_directional_oaa', 'statcast', statcast_outfield_directional_oaa, 2008, limit=5, 
                                    current_year=True, kwargs={'min_opp':0}, add_year=True)
    _outfielder_directional.update_table()

    logging.info('Finished updating statcast data')

if __name__ == "__main__":
    configure_logging()
    main()
