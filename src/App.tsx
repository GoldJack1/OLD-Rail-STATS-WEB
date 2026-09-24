import React, { useEffect } from 'react'
import { Routes, Route, useLocation } from 'react-router-dom'
import { AuthProvider } from './contexts/AuthContext'
import { StationCollectionProvider } from './contexts/StationCollectionContext'
import { PendingStationChangesProvider } from './contexts/PendingStationChangesContext'
import HomePage from './pages/HomePage/HomePage'
import LoginPage from './pages/LoginPage/LoginPage'
import StationsPageRefactored from './pages/StationsPageRefactored/StationsPageRefactored'
import StationsMapPage from './pages/StationsMapPage/StationsMapPage'
import ReviewPendingChangesPage from './pages/ReviewPendingChangesPage/ReviewPendingChangesPage'
import StationDetailsPage from './pages/StationDetailsPage/StationDetailsPage'
import LegacyStationRedirect from './pages/StationDetailsPage/LegacyStationRedirect'
import NewStationPage from './pages/NewStationPage'
import MigrationPage from './pages/MigrationPage/MigrationPage'
import DesignSystemHomePage from './pages/designSystem/DesignSystemHomePage/DesignSystemHomePage'
import ColoursPage from './pages/designSystem/ColoursPage/ColoursPage'
import TypographyPage from './pages/designSystem/TypographyPage/TypographyPage'
import ButtonsPage from './pages/designSystem/ButtonsPage/ButtonsPage'
import LayoutPage from './pages/designSystem/LayoutPage/LayoutPage'
import ComponentsPage from './pages/designSystem/ComponentsPage/ComponentsPage'
import IconsPage from './pages/designSystem/IconsPage/IconsPage'
import HerosPage from './pages/designSystem/HerosPage/HerosPage'
import PrivacyPolicyPage from './pages/legal/PrivacyPolicyPage/PrivacyPolicyPage'
import EulaPage from './pages/legal/EulaPage/EulaPage'
import MessageCentreAdminPage from './pages/MessageCentreAdminPage/MessageCentreAdminPage'
import MessageCentreDashboardPage from './pages/MessageCentreDashboardPage'
import DarwinDeparturesPage from './pages/DarwinDeparturesPage'
import ServiceDetailPage from './pages/ServiceDetailPage'
import UnitLookupPage from './pages/UnitLookupPage'
import UnitsInServicePage from './pages/UnitsInServicePage'
import ApiStatusPage from './pages/ApiStatusPage'
import BashPlannerPage from './pages/BashPlannerPage'
import NotFoundPage from './pages/NotFoundPage'
import Header from './components/misc/Header/Header'
import Footer from './components/misc/Footer/Footer'
import { ProtectedRoute } from './components/firebase'
import './styles/App.css'

const PAGE_TITLES: Record<string, string> = {
  '/': 'Rail Statistics',
  '/home': 'Home | Rail Statistics',
  '/log-in': 'Log in | Rail Statistics',
  '/stations': 'Stations | Rail Statistics',
  '/stations/map': 'Map | Rail Statistics',
  '/stations/edit': 'Stations Admin | Rail Statistics',
  '/stations/pending-review': 'Review changes | Rail Statistics',
  '/stations/new': 'New Station | Rail Statistics',
  '/migration': 'Migration | Rail Statistics',
  '/buttons': 'Button Components | Rail Statistics',
  '/design-system': 'Design System | Rail Statistics',
  '/design-system/colours': 'Design System Colours | Rail Statistics',
  '/design-system/typography': 'Design System Typography | Rail Statistics',
  '/design-system/buttons': 'Design System Buttons | Rail Statistics',
  '/design-system/layout': 'Design System Layout | Rail Statistics',
  '/design-system/components': 'Design System Components | Rail Statistics',
  '/design-system/icons': 'Design System Icons | Rail Statistics',
  '/design-system/heros': 'Design System Heroes | Rail Statistics',
  '/admin/messages': 'Message Centre Admin | Rail Statistics',
  '/departures': 'Live Departures | Rail Statistics',
  '/services': 'Service Detail | Rail Statistics',
  '/units': 'Units in Service | Rail Statistics',
  '/bash': 'Station Bash | Rail Statistics',
  '/api-status': 'API Status | Rail Statistics',
  '/privacy': 'Privacy Policy | Rail Statistics',
  '/eula': 'EULA | Rail Statistics',
}

const App: React.FC = () => {
  const { pathname } = useLocation()
  const isStationsPage =
    pathname === '/stations' ||
    pathname === '/stations/edit' ||
    pathname === '/stations/map'

  useEffect(() => {
    if (pathname.startsWith('/units/')) {
      document.title = 'Unit Detail | Rail Statistics'
    } else if (pathname.startsWith('/services/')) {
      document.title = 'Service Detail | Rail Statistics'
    } else {
      document.title = PAGE_TITLES[pathname] ?? 'Rail Statistics'
    }
    window.scrollTo({ top: 0, behavior: 'instant' })
  }, [pathname])

  return (
    <AuthProvider>
      <StationCollectionProvider>
        <PendingStationChangesProvider>
          <div className="app">
            <Header />
            <main className={`main-content app-main${isStationsPage ? ' app-main--stations-layout' : ''}`}>
              <Routes>
                <Route path="/" element={<HomePage />} />
                <Route path="/home" element={<HomePage />} />
                <Route path="/log-in" element={<LoginPage />} />
                <Route path="/stations/map" element={<StationsMapPage />} />
                <Route path="/stations" element={<ProtectedRoute requireFullAdmin><StationsPageRefactored /></ProtectedRoute>} />
                <Route path="/stations/edit" element={<ProtectedRoute requireFullAdmin><StationsPageRefactored initialMode="edit" /></ProtectedRoute>} />
                <Route path="/stations/pending-review" element={<ProtectedRoute requireFullAdmin><ReviewPendingChangesPage /></ProtectedRoute>} />
                <Route path="/stations/new" element={<ProtectedRoute requireFullAdmin><NewStationPage /></ProtectedRoute>} />
                <Route path="/stations/:network/:stationSlug" element={<StationDetailsPage mode="view" />} />
                <Route path="/stations/:network/:stationSlug/edit" element={<ProtectedRoute requireFullAdmin><StationDetailsPage mode="edit" /></ProtectedRoute>} />
                <Route path="/stations/:legacyStationId/edit" element={<ProtectedRoute requireFullAdmin><LegacyStationRedirect mode="edit" /></ProtectedRoute>} />
                <Route path="/stations/:legacyStationId" element={<LegacyStationRedirect mode="view" />} />
                <Route path="/migration" element={<MigrationPage />} />
                <Route path="/buttons" element={<ButtonsPage />} />
                <Route path="/design-system" element={<ProtectedRoute requireFullAdmin><DesignSystemHomePage /></ProtectedRoute>} />
                <Route path="/design-system/colours" element={<ProtectedRoute requireFullAdmin><ColoursPage /></ProtectedRoute>} />
                <Route path="/design-system/typography" element={<ProtectedRoute requireFullAdmin><TypographyPage /></ProtectedRoute>} />
                <Route path="/design-system/buttons" element={<ProtectedRoute requireFullAdmin><ButtonsPage /></ProtectedRoute>} />
                <Route path="/design-system/layout" element={<ProtectedRoute requireFullAdmin><LayoutPage /></ProtectedRoute>} />
                <Route path="/design-system/components" element={<ProtectedRoute requireFullAdmin><ComponentsPage /></ProtectedRoute>} />
                <Route path="/design-system/icons" element={<ProtectedRoute requireFullAdmin><IconsPage /></ProtectedRoute>} />
                <Route path="/design-system/heros" element={<ProtectedRoute requireFullAdmin><HerosPage /></ProtectedRoute>} />
                <Route path="/admin/messages" element={<ProtectedRoute requireFullAdmin><MessageCentreDashboardPage /></ProtectedRoute>} />
                <Route path="/admin/messages/new" element={<ProtectedRoute requireFullAdmin><MessageCentreAdminPage /></ProtectedRoute>} />
                <Route path="/admin/messages/:messageId" element={<ProtectedRoute requireFullAdmin><MessageCentreAdminPage /></ProtectedRoute>} />
                <Route path="/departures" element={<DarwinDeparturesPage />} />
                <Route path="/departures/:code" element={<DarwinDeparturesPage />} />
                <Route path="/services/:rid" element={<ServiceDetailPage />} />
                <Route path="/units" element={<UnitsInServicePage />} />
                <Route path="/units/:unitId" element={<UnitLookupPage />} />
                <Route path="/bash" element={<ProtectedRoute requireDarwinTools><BashPlannerPage /></ProtectedRoute>} />
                <Route path="/api-status" element={<ProtectedRoute requireDarwinTools><ApiStatusPage /></ProtectedRoute>} />
                <Route path="/privacy" element={<PrivacyPolicyPage />} />
                <Route path="/eula" element={<EulaPage />} />
                <Route path="*" element={<NotFoundPage />} />
              </Routes>
            </main>
            <Footer />
          </div>
        </PendingStationChangesProvider>
      </StationCollectionProvider>
    </AuthProvider>
  )
}

export default App
