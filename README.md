# ![ROCED][logo]
**R**esponsive **O**n-Demand **C**loud-**e**nabled **D**eployment is a tool which can interface with different batch systems (Torque, HTCondor) and cloud sites (Eucalyptus, OpenNebula, OpenStack, Amazon EC2, etc.).
It monitors demand of computing resources in the batch system(s) and dynamically manages **V**irtual **M**achines (starting and terminating them) on different cloud sites.

## Design

ROCED periodically runs a management cycle, where it performs three steps:
* Monitor batch system's queue and determine demand for machines
* Boot machines
* Integrate booted machines into batch system

![Visualisation of management cycle][workflow]

ROCED consists of five components; everything except the core has a modular structure, in order to offer a maximum of flexibility.  
Users can freely combine different adapters to fulfill their requirement or even write their own.  

ROCED needs at least one of each component to be in any way useful and we advice to use _Requirement Adapter_ and _Integration Adapter_ for the same batch system.
* **Core**
* **Requirement Adapters**  
  Monitor batch system(s) to determine the demand for machines.
* **Site Adapters**  
Request machines at cloud site(s)
* **Integration Adapters**  
(Dis-)Integrate running machines from/into batch system(s)
* **Broker**  
Balance demand across different cloud sites, depending on different metrics (e.g.: cost)

![Visualisation of modular components][design]

## Requirements/Installation
* Python 2.7 or 3.3+
    * Python 2 _requires_ the [future](http://python-future.org/) and the [configparser](https://pypi.python.org/pypi/configparser) package
    * Various adapters have system/site dependant packages.  
We follow the [PEP 8](https://www.python.org/dev/peps/pep-0008/#imports) guideline when listing module imports, so you you can easily identify the needed modules for each adapter.  
        * [novaclient](https://pypi.python.org/pypi/python-novaclient/) is required to interface with _OpenStack_.
        * [HTCondor Python bindings](https://research.cs.wisc.edu/htcondor/manual/latest/6_7Python_Bindings.html) are useful to interface with _HTCondor_.
* Correctly set up batch system
* VM image(s) which can integrate into batch system(s) as worker node(s).

# Contributors
ROCED was developed at the _Institut für Experimentelle Kernphysik_ at the _Karlsruhe Institute of Technology_.  
Further information can be found in the `doc` folder.
# License
    ROCED is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    ROCED is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with ROCED.  If not, see <http://www.gnu.org/licenses/>.

[logo]: https://cdn.rawgit.com/roced-scheduler/ROCED/master/doc/roced_logo.svg
[design]: https://cdn.rawgit.com/roced-scheduler/ROCED/master/doc/roced_design.svg
[workflow]: https://cdn.rawgit.com/roced-scheduler/ROCED/master/doc/roced_workflow.svg
